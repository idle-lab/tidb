// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"math"
	"regexp"
	"slices"
	"strings"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/statistics"
	statsstorage "github.com/pingcap/tidb/pkg/statistics/handle/storage"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/tikv/client-go/v2/oracle"
)

const (
	statsMemTableMaxBatchRows  = 1024
	statsMemTableMaxBatchBytes = 8 << 20
	statsMemTableIDsPerQuery   = 256
)

type statsObjectKey struct {
	physicalID int64
	isIndex    int64
	histID     int64
}

type statsPhysicalTable struct {
	tableID       int64
	physicalID    int64
	dbName        string
	tableName     string
	partitionName string
}

type statsObject struct {
	physical         *statsPhysicalTable
	name             string
	columnInfo       *model.ColumnInfo
	indexInfo        *model.IndexInfo
	indexColumnTypes []byte
	isHandle         bool
}

type statsNameFilter struct {
	exact    set.StringSet
	patterns []*regexp.Regexp
}

func newStatsNameFilter(exact set.StringSet, patterns []string) (statsNameFilter, error) {
	filter := statsNameFilter{exact: exact, patterns: make([]*regexp.Regexp, 0, len(patterns))}
	for _, pattern := range patterns {
		compiled, err := regexp.Compile(pattern)
		if err != nil {
			return statsNameFilter{}, errors.Trace(err)
		}
		filter.patterns = append(filter.patterns, compiled)
	}
	return filter, nil
}

func (f statsNameFilter) match(name string) bool {
	name = strings.ToLower(name)
	if len(f.exact) > 0 && !f.exact.Exist(name) {
		return false
	}
	for _, pattern := range f.patterns {
		if !pattern.MatchString(name) {
			return false
		}
	}
	return true
}

type statsQuery struct {
	sql  string
	args []any
}

type statsRowSource interface {
	Open(context.Context, sessionctx.Context, uint64, []statsQuery, *memory.Tracker) error
	NextBatch(context.Context, int, int64) ([][]types.Datum, bool, error)
	Close() error
}

type sqlRecordSetStatsSource struct {
	pool     util.DestroyableSessionPool
	resource pools.Resource
	session  sessionctx.Context

	queries   []statsQuery
	queryIdx  int
	rs        sqlexec.RecordSet
	chk       *chunk.Chunk
	rowIdx    int
	fieldTps  []*types.FieldType
	done      bool
	unhealthy bool

	memTracker   *memory.Tracker
	chunkMemSize int64
}

func (s *sqlRecordSetStatsSource) Open(
	ctx context.Context,
	sctx sessionctx.Context,
	readTS uint64,
	queries []statsQuery,
	memTracker *memory.Tracker,
) error {
	if s.session != nil {
		return errors.New("stats row source is already open")
	}
	s.pool = domain.GetDomain(sctx).SysSessionPool()
	resource, err := s.pool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	s.resource = resource
	s.session = resource.(sessionctx.Context)
	s.session.GetSessionVars().InRestrictedSQL = true
	s.queries = queries
	s.memTracker = memTracker

	internalCtx := kv.WithInternalSourceType(ctx, kv.InternalTxnStatsForegroundPriority)
	if _, err = s.session.GetSQLExecutor().ExecuteInternal(
		internalCtx,
		"SET @@session.tidb_snapshot = %?",
		readTS,
	); err != nil {
		s.pool.Destroy(s.resource)
		s.resource = nil
		s.session = nil
		return errors.Trace(err)
	}
	return nil
}

func (s *sqlRecordSetStatsSource) openNextRecordSet(ctx context.Context) error {
	for s.queryIdx < len(s.queries) {
		query := s.queries[s.queryIdx]
		s.queryIdx++
		internalCtx := kv.WithInternalSourceType(ctx, kv.InternalTxnStatsForegroundPriority)
		rs, err := s.session.GetSQLExecutor().ExecuteInternal(internalCtx, query.sql, query.args...)
		if err != nil {
			s.unhealthy = true
			return errors.Trace(err)
		}
		if rs == nil {
			continue
		}
		s.rs = rs
		s.chk = rs.NewChunk(nil)
		s.chunkMemSize = s.chk.MemoryUsage()
		s.memTracker.Consume(s.chunkMemSize)
		s.rowIdx = 0
		fields := rs.Fields()
		s.fieldTps = make([]*types.FieldType, len(fields))
		for i, field := range fields {
			s.fieldTps[i] = &field.Column.FieldType
		}
		return nil
	}
	s.done = true
	return nil
}

func (s *sqlRecordSetStatsSource) closeRecordSet() error {
	if s.rs == nil {
		return nil
	}
	err := s.rs.Close()
	s.rs = nil
	s.chk = nil
	s.rowIdx = 0
	s.fieldTps = nil
	if s.chunkMemSize != 0 {
		s.memTracker.Consume(-s.chunkMemSize)
		s.chunkMemSize = 0
	}
	return err
}

func (s *sqlRecordSetStatsSource) NextBatch(
	ctx context.Context,
	maxRows int,
	maxBytes int64,
) (rows [][]types.Datum, done bool, err error) {
	if s.done {
		return nil, true, nil
	}
	if s.session == nil {
		return nil, false, errors.New("stats row source is not open")
	}
	if maxRows <= 0 {
		maxRows = statsMemTableMaxBatchRows
	}
	rows = make([][]types.Datum, 0, maxRows)
	var usedBytes int64
	defer func() {
		if err != nil && usedBytes != 0 {
			s.memTracker.Consume(-usedBytes)
		}
	}()

	for len(rows) < maxRows {
		if err := ctx.Err(); err != nil {
			s.unhealthy = true
			return nil, false, err
		}
		if s.rs == nil {
			if err := s.openNextRecordSet(ctx); err != nil {
				return nil, false, err
			}
			if s.done {
				return rows, true, nil
			}
		}

		if s.rowIdx >= s.chk.NumRows() {
			oldChunkSize := s.chk.MemoryUsage()
			s.chk.Reset()
			s.chk.SetRequiredRows(maxRows-len(rows), maxRows)
			nextErr := s.rs.Next(ctx, s.chk)
			newChunkSize := s.chk.MemoryUsage()
			s.memTracker.Consume(newChunkSize - oldChunkSize)
			s.chunkMemSize += newChunkSize - oldChunkSize
			if nextErr != nil {
				s.unhealthy = true
				return nil, false, errors.Trace(nextErr)
			}
			s.rowIdx = 0
			if s.chk.NumRows() == 0 {
				if err := s.closeRecordSet(); err != nil {
					return nil, false, errors.Trace(err)
				}
				continue
			}
		}

		row := types.CloneRow(s.chk.GetRow(s.rowIdx).GetDatumRow(s.fieldTps))
		rowBytes := types.EstimatedMemUsage(row, 1)
		if len(rows) > 0 && maxBytes > 0 && usedBytes+rowBytes > maxBytes {
			break
		}
		rows = append(rows, row)
		s.memTracker.Consume(rowBytes)
		usedBytes += rowBytes
		s.rowIdx++
	}
	return rows, false, nil
}

func (s *sqlRecordSetStatsSource) Close() error {
	var firstErr error
	if err := s.closeRecordSet(); err != nil {
		firstErr = err
	}
	if s.session == nil {
		return firstErr
	}
	resetCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStatsForegroundPriority)
	if _, err := s.session.GetSQLExecutor().ExecuteInternal(resetCtx, "SET @@session.tidb_snapshot = ''"); err != nil {
		if firstErr == nil {
			firstErr = err
		}
		s.pool.Destroy(s.resource)
	} else if firstErr != nil || s.unhealthy {
		s.pool.Destroy(s.resource)
	} else {
		s.pool.Put(s.resource)
	}
	s.resource = nil
	s.session = nil
	return firstErr
}

type statsMemTableRetriever struct {
	dummyCloser

	table      *model.TableInfo
	outputCols []*model.ColumnInfo
	extractor  *plannercore.StatsTableExtractor
	memTracker *memory.Tracker

	initialized bool
	done        bool
	source      statsRowSource
	lastFetch   int64

	physicalByID map[int64]*statsPhysicalTable
	objectsByKey map[statsObjectKey]*statsObject

	lastBucketKey statsObjectKey
	hasBucketKey  bool
	cumulative    int64
}

func (e *statsMemTableRetriever) retrieve(
	ctx context.Context,
	sctx sessionctx.Context,
) ([][]types.Datum, error) {
	if e.done || e.extractor.SkipRequest {
		return nil, nil
	}
	if !e.initialized {
		if err := e.initialize(ctx, sctx); err != nil {
			return nil, err
		}
	}
	if e.lastFetch != 0 {
		e.memTracker.Consume(-e.lastFetch)
		e.lastFetch = 0
	}
	rawRows, done, err := e.source.NextBatch(ctx, statsMemTableMaxBatchRows, statsMemTableMaxBatchBytes)
	if err != nil {
		return nil, err
	}
	if len(rawRows) == 0 {
		e.done = done
		return nil, nil
	}
	rawSize := statsRowsMemoryUsage(rawRows)
	rows := make([][]types.Datum, 0, len(rawRows))
	for _, rawRow := range rawRows {
		row, err := e.enrichRow(sctx, rawRow)
		if err != nil {
			e.memTracker.Consume(-rawSize)
			return nil, err
		}
		if row != nil {
			rows = append(rows, row)
		}
	}
	finalSize := statsRowsMemoryUsage(rows)
	e.memTracker.Consume(finalSize - rawSize)
	e.lastFetch = finalSize
	e.done = done
	return rows, nil
}

func (e *statsMemTableRetriever) close() error {
	if e.lastFetch != 0 {
		e.memTracker.Consume(-e.lastFetch)
		e.lastFetch = 0
	}
	if e.source != nil {
		return e.source.Close()
	}
	return nil
}

func statsRowsMemoryUsage(rows [][]types.Datum) int64 {
	var size int64
	for _, row := range rows {
		size += types.EstimatedMemUsage(row, 1)
	}
	return size
}

func (e *statsMemTableRetriever) initialize(ctx context.Context, sctx sessionctx.Context) error {
	e.initialized = true
	readTS, err := sessiontxn.GetTxnManager(sctx).GetStmtReadTS()
	if err != nil {
		return errors.Trace(err)
	}
	snapshotIS, err := domain.GetDomain(sctx).GetSnapshotInfoSchema(readTS)
	if err != nil {
		return errors.Trace(err)
	}

	schemaFilter, err := newStatsNameFilter(e.extractor.TableSchema, e.extractor.TableSchemaPatterns)
	if err != nil {
		return err
	}
	tableFilter, err := newStatsNameFilter(e.extractor.TableName, e.extractor.TableNamePatterns)
	if err != nil {
		return err
	}
	partitionFilter, err := newStatsNameFilter(e.extractor.PartitionName, e.extractor.PartitionNamePatterns)
	if err != nil {
		return err
	}
	columnFilter, err := newStatsNameFilter(e.extractor.ColumnName, e.extractor.ColumnNamePatterns)
	if err != nil {
		return err
	}

	e.physicalByID = make(map[int64]*statsPhysicalTable)
	e.objectsByKey = make(map[statsObjectKey]*statsObject)
	for _, dbName := range snapshotIS.AllSchemaNames() {
		if !schemaFilter.match(dbName.O) {
			continue
		}
		tables, err := snapshotIS.SchemaTableInfos(ctx, dbName)
		if err != nil {
			return errors.Trace(err)
		}
		for _, tableInfo := range tables {
			if !tableFilter.match(tableInfo.Name.O) || !statsInt64SetMatch(e.extractor.TableIDs, tableInfo.ID) {
				continue
			}
			partitionInfo := tableInfo.GetPartitionInfo()
			if partitionInfo == nil {
				e.addStatsPhysicalTable(dbName.O, tableInfo, tableInfo.ID, "", partitionFilter, columnFilter)
				continue
			}
			e.addStatsPhysicalTable(dbName.O, tableInfo, tableInfo.ID, "global", partitionFilter, columnFilter)
			for _, definition := range partitionInfo.Definitions {
				e.addStatsPhysicalTable(dbName.O, tableInfo, definition.ID, definition.Name.O, partitionFilter, columnFilter)
			}
		}
	}
	if len(e.physicalByID) == 0 {
		e.done = true
		e.source = &sqlRecordSetStatsSource{done: true}
		return nil
	}

	queries := e.buildStatsQueries()
	if len(queries) == 0 {
		e.done = true
		e.source = &sqlRecordSetStatsSource{done: true}
		return nil
	}
	e.source = &sqlRecordSetStatsSource{}
	return e.source.Open(ctx, sctx, readTS, queries, e.memTracker)
}

func statsInt64SetMatch(values set.Int64Set, value int64) bool {
	return len(values) == 0 || values.Exist(value)
}

func (e *statsMemTableRetriever) addStatsPhysicalTable(
	dbName string,
	tableInfo *model.TableInfo,
	physicalID int64,
	partitionName string,
	partitionFilter statsNameFilter,
	columnFilter statsNameFilter,
) {
	if !partitionFilter.match(partitionName) || !statsInt64SetMatch(e.extractor.PhysicalIDs, physicalID) {
		return
	}
	physical := &statsPhysicalTable{
		tableID:       tableInfo.ID,
		physicalID:    physicalID,
		dbName:        dbName,
		tableName:     tableInfo.Name.O,
		partitionName: partitionName,
	}
	e.physicalByID[physicalID] = physical
	if e.table.Name.O == infoschema.TableTiDBStatsMeta {
		return
	}

	if statsInt64SetMatch(e.extractor.IsIndexes, 0) {
		for _, columnInfo := range tableInfo.Columns {
			if !columnFilter.match(columnInfo.Name.O) || !statsInt64SetMatch(e.extractor.HistogramIDs, columnInfo.ID) {
				continue
			}
			key := statsObjectKey{physicalID: physicalID, histID: columnInfo.ID}
			e.objectsByKey[key] = &statsObject{
				physical:   physical,
				name:       columnInfo.Name.O,
				columnInfo: columnInfo,
				isHandle:   tableInfo.PKIsHandle && mysql.HasPriKeyFlag(columnInfo.GetFlag()),
			}
		}
	}
	if !statsInt64SetMatch(e.extractor.IsIndexes, 1) {
		return
	}
	for _, indexInfo := range tableInfo.Indices {
		if !columnFilter.match(indexInfo.Name.O) || !statsInt64SetMatch(e.extractor.HistogramIDs, indexInfo.ID) {
			continue
		}
		columnTypes := make([]byte, 0, len(indexInfo.Columns))
		valid := true
		for _, indexColumn := range indexInfo.Columns {
			if indexColumn.Offset < 0 || indexColumn.Offset >= len(tableInfo.Columns) {
				valid = false
				break
			}
			columnTypes = append(columnTypes, tableInfo.Columns[indexColumn.Offset].FieldType.GetType())
		}
		if !valid {
			continue
		}
		key := statsObjectKey{physicalID: physicalID, isIndex: 1, histID: indexInfo.ID}
		e.objectsByKey[key] = &statsObject{
			physical:         physical,
			name:             indexInfo.Name.O,
			indexInfo:        indexInfo,
			indexColumnTypes: columnTypes,
		}
	}
}

func (e *statsMemTableRetriever) buildStatsQueries() []statsQuery {
	if e.table.Name.O == infoschema.TableTiDBStatsMeta {
		ids := make([]int64, 0, len(e.physicalByID))
		for id := range e.physicalByID {
			ids = append(ids, id)
		}
		slices.Sort(ids)
		return buildStatsMetaQueries(ids)
	}
	keys := make([]statsObjectKey, 0, len(e.objectsByKey))
	for key := range e.objectsByKey {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, compareStatsObjectKey)
	return buildStatsObjectQueries(e.table.Name.O, keys)
}

func compareStatsObjectKey(a, b statsObjectKey) int {
	if a.physicalID != b.physicalID {
		if a.physicalID < b.physicalID {
			return -1
		}
		return 1
	}
	if a.isIndex != b.isIndex {
		if a.isIndex < b.isIndex {
			return -1
		}
		return 1
	}
	if a.histID < b.histID {
		return -1
	}
	if a.histID > b.histID {
		return 1
	}
	return 0
}

func buildStatsMetaQueries(ids []int64) []statsQuery {
	queries := make([]statsQuery, 0, (len(ids)+statsMemTableIDsPerQuery-1)/statsMemTableIDsPerQuery)
	for start := 0; start < len(ids); start += statsMemTableIDsPerQuery {
		end := min(start+statsMemTableIDsPerQuery, len(ids))
		placeholders := strings.TrimSuffix(strings.Repeat("%?,", end-start), ",")
		sql := "SELECT m.table_id, m.version, m.modify_count, m.count, " +
			"CAST(GREATEST(m.snapshot, COALESCE(MAX(IF(h.stats_ver != 0, h.version, 0)), 0)) AS UNSIGNED) " +
			"FROM mysql.stats_meta m LEFT JOIN mysql.stats_histograms h ON h.table_id = m.table_id " +
			"WHERE m.table_id IN (" + placeholders + ") " +
			"GROUP BY m.table_id, m.version, m.modify_count, m.count, m.snapshot ORDER BY m.table_id"
		args := make([]any, 0, end-start)
		for _, id := range ids[start:end] {
			args = append(args, id)
		}
		queries = append(queries, statsQuery{sql: sql, args: args})
	}
	return queries
}

func buildStatsObjectQueries(tableName string, keys []statsObjectKey) []statsQuery {
	queries := make([]statsQuery, 0, (len(keys)+statsMemTableIDsPerQuery-1)/statsMemTableIDsPerQuery)
	for start := 0; start < len(keys); start += statsMemTableIDsPerQuery {
		end := min(start+statsMemTableIDsPerQuery, len(keys))
		placeholders := strings.TrimSuffix(strings.Repeat("(%?,%?,%?),", end-start), ",")
		var sql string
		switch tableName {
		case infoschema.TableTiDBStatsHistograms:
			sql = "SELECT h.table_id, h.is_index, h.hist_id, h.distinct_count, h.null_count, " +
				"h.tot_col_size, h.version, h.stats_ver, h.correlation, COALESCE(m.count, 0), " +
				"CAST(COALESCE((SELECT SUM(b.count) FROM mysql.stats_buckets b WHERE b.table_id=h.table_id AND b.is_index=h.is_index AND b.hist_id=h.hist_id), 0) AS SIGNED), " +
				"CAST(COALESCE((SELECT SUM(n.count) FROM mysql.stats_top_n n WHERE n.table_id=h.table_id AND n.is_index=h.is_index AND n.hist_id=h.hist_id), 0) AS SIGNED) " +
				"FROM mysql.stats_histograms h LEFT JOIN mysql.stats_meta m ON m.table_id=h.table_id " +
				"WHERE (h.table_id,h.is_index,h.hist_id) IN (" + placeholders + ") " +
				"ORDER BY h.table_id,h.is_index,h.hist_id"
		case infoschema.TableTiDBStatsBuckets:
			sql = "SELECT table_id,is_index,hist_id,bucket_id,count,repeats,lower_bound,upper_bound,ndv " +
				"FROM mysql.stats_buckets WHERE (table_id,is_index,hist_id) IN (" + placeholders + ") " +
				"ORDER BY table_id,is_index,hist_id,bucket_id"
		case infoschema.TableTiDBStatsTopN:
			sql = "SELECT table_id,is_index,hist_id,value,count FROM mysql.stats_top_n " +
				"WHERE (table_id,is_index,hist_id) IN (" + placeholders + ") " +
				"ORDER BY table_id,is_index,hist_id"
		default:
			return nil
		}
		args := make([]any, 0, (end-start)*3)
		for _, key := range keys[start:end] {
			args = append(args, key.physicalID, key.isIndex, key.histID)
		}
		queries = append(queries, statsQuery{sql: sql, args: args})
	}
	return queries
}

func (e *statsMemTableRetriever) enrichRow(
	sctx sessionctx.Context,
	raw []types.Datum,
) ([]types.Datum, error) {
	switch e.table.Name.O {
	case infoschema.TableTiDBStatsMeta:
		return e.enrichStatsMetaRow(raw), nil
	case infoschema.TableTiDBStatsHistograms:
		return e.enrichStatsHistogramRow(sctx, raw)
	case infoschema.TableTiDBStatsBuckets:
		return e.enrichStatsBucketRow(sctx, raw)
	case infoschema.TableTiDBStatsTopN:
		return e.enrichStatsTopNRow(sctx, raw)
	default:
		return nil, errors.Errorf("unsupported stats virtual table %s", e.table.Name.O)
	}
}

func (e *statsMemTableRetriever) enrichStatsMetaRow(raw []types.Datum) []types.Datum {
	physicalID := raw[0].GetInt64()
	physical := e.physicalByID[physicalID]
	if physical == nil {
		return nil
	}
	lastAnalyzeVersion := raw[4].GetUint64()
	lastAnalyze := types.NewDatum(nil)
	if lastAnalyzeVersion != 0 {
		lastAnalyze = types.NewDatum(statsVersionToTime(lastAnalyzeVersion))
	}
	fullRow := makeStatsDatums(
		physical.tableID,
		physical.physicalID,
		physical.dbName,
		physical.tableName,
		physical.partitionName,
		statsVersionToTime(raw[1].GetUint64()),
		raw[2],
		raw[3],
		lastAnalyze,
	)
	return e.projectStatsRow(fullRow)
}

func (e *statsMemTableRetriever) enrichStatsHistogramRow(
	sctx sessionctx.Context,
	raw []types.Datum,
) ([]types.Datum, error) {
	key := statsObjectKey{
		physicalID: raw[0].GetInt64(),
		isIndex:    raw[1].GetInt64(),
		histID:     raw[2].GetInt64(),
	}
	object := e.objectsByKey[key]
	if object == nil {
		return nil, nil
	}
	avgColSize := float64(0)
	if key.isIndex == 0 {
		rowCount, err := raw[9].ToInt64(sctx.GetSessionVars().StmtCtx.TypeCtx())
		if err != nil {
			return nil, errors.Trace(err)
		}
		avgColSize = statsHistogramAvgColSize(
			object,
			rowCount,
			raw[4].GetInt64(),
			raw[5].GetInt64(),
			raw[7].GetInt64(),
			raw[10].GetInt64(),
			raw[11].GetInt64(),
		)
	}
	physical := object.physical
	fullRow := makeStatsDatums(
		physical.tableID,
		physical.physicalID,
		physical.dbName,
		physical.tableName,
		physical.partitionName,
		object.name,
		key.isIndex,
		key.histID,
		statsVersionToTime(raw[6].GetUint64()),
		raw[7],
		raw[3],
		raw[4],
		avgColSize,
		raw[8],
	)
	return e.projectStatsRow(fullRow), nil
}

func statsHistogramAvgColSize(
	object *statsObject,
	rowCount int64,
	nullCount int64,
	totalColumnSize int64,
	statsVersion int64,
	bucketCount int64,
	topNCount int64,
) float64 {
	if rowCount == 0 {
		return 0
	}
	if object.isHandle {
		return 8
	}
	histogramCount := bucketCount + nullCount
	if statsVersion >= statistics.Version2 {
		histogramCount += topNCount
	}
	notNullRatio := 1.0
	if histogramCount > 0 {
		notNullRatio = max(0, 1-float64(nullCount)/float64(histogramCount))
	}
	switch object.columnInfo.FieldType.GetType() {
	case mysql.TypeFloat, mysql.TypeDouble, mysql.TypeDuration, mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		return 8 * notNullRatio
	}
	return max(0, math.Round(float64(totalColumnSize)/float64(rowCount)*100)/100)
}

func (e *statsMemTableRetriever) enrichStatsBucketRow(
	sctx sessionctx.Context,
	raw []types.Datum,
) ([]types.Datum, error) {
	key := statsObjectKey{
		physicalID: raw[0].GetInt64(),
		isIndex:    raw[1].GetInt64(),
		histID:     raw[2].GetInt64(),
	}
	object := e.objectsByKey[key]
	if object == nil {
		return nil, nil
	}
	if !e.hasBucketKey || e.lastBucketKey != key {
		e.lastBucketKey = key
		e.hasBucketKey = true
		e.cumulative = 0
	}
	e.cumulative += raw[4].GetInt64()
	lowerBound, err := statsBucketValueToDatum(sctx, object, raw[6])
	if err != nil {
		return nil, err
	}
	upperBound, err := statsBucketValueToDatum(sctx, object, raw[7])
	if err != nil {
		return nil, err
	}
	physical := object.physical
	fullRow := makeStatsDatums(
		physical.tableID,
		physical.physicalID,
		physical.dbName,
		physical.tableName,
		physical.partitionName,
		object.name,
		key.isIndex,
		key.histID,
		raw[3],
		e.cumulative,
		raw[5],
		lowerBound,
		upperBound,
		raw[8],
	)
	return e.projectStatsRow(fullRow), nil
}

func statsBucketValueToDatum(
	sctx sessionctx.Context,
	object *statsObject,
	value types.Datum,
) (types.Datum, error) {
	if value.IsNull() {
		return value, nil
	}
	if object.indexInfo != nil {
		str, err := statistics.ValueToString(
			sctx.GetSessionVars(),
			&value,
			len(object.indexInfo.Columns),
			object.indexColumnTypes,
		)
		if err != nil {
			return types.Datum{}, errors.Trace(err)
		}
		return types.NewStringDatum(str), nil
	}
	fieldType := object.columnInfo.FieldType
	if fieldType.EvalType() == types.ETString &&
		fieldType.GetType() != mysql.TypeEnum && fieldType.GetType() != mysql.TypeSet {
		fieldType = *types.NewFieldType(mysql.TypeBlob)
	}
	converted, err := statsstorage.ConvertBoundFromBlob(
		statistics.UTCWithAllowInvalidDateCtx,
		value,
		&fieldType,
	)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	str, err := converted.ToString()
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return types.NewStringDatum(str), nil
}

func (e *statsMemTableRetriever) enrichStatsTopNRow(
	sctx sessionctx.Context,
	raw []types.Datum,
) ([]types.Datum, error) {
	key := statsObjectKey{
		physicalID: raw[0].GetInt64(),
		isIndex:    raw[1].GetInt64(),
		histID:     raw[2].GetInt64(),
	}
	object := e.objectsByKey[key]
	if object == nil {
		return nil, nil
	}
	value := raw[3]
	displayValue := value
	if !value.IsNull() {
		var numColumns int
		var columnTypes []byte
		if object.indexInfo != nil {
			numColumns = len(object.indexInfo.Columns)
			columnTypes = object.indexColumnTypes
		} else {
			numColumns = 1
			columnTypes = []byte{object.columnInfo.FieldType.GetType()}
		}
		str, err := statistics.ValueToString(sctx.GetSessionVars(), &value, numColumns, columnTypes)
		if err != nil {
			return nil, errors.Trace(err)
		}
		displayValue = types.NewStringDatum(str)
	}
	physical := object.physical
	fullRow := makeStatsDatums(
		physical.tableID,
		physical.physicalID,
		physical.dbName,
		physical.tableName,
		physical.partitionName,
		object.name,
		key.isIndex,
		key.histID,
		displayValue,
		raw[4],
	)
	return e.projectStatsRow(fullRow), nil
}

func (e *statsMemTableRetriever) projectStatsRow(fullRow []types.Datum) []types.Datum {
	if len(e.outputCols) == len(e.table.Columns) {
		return fullRow
	}
	row := make([]types.Datum, len(e.outputCols))
	for i, column := range e.outputCols {
		row[i] = fullRow[column.Offset]
	}
	return row
}

func makeStatsDatums(values ...any) []types.Datum {
	row := make([]types.Datum, len(values))
	for i, value := range values {
		if datum, ok := value.(types.Datum); ok {
			row[i] = datum
		} else {
			row[i] = types.NewDatum(value)
		}
	}
	return row
}

func statsVersionToTime(version uint64) types.Time {
	return types.NewTime(types.FromGoTime(oracle.GetTimeFromTS(version)), mysql.TypeDatetime, 0)
}
