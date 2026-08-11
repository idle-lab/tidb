# Add full persistent statistics virtual tables

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan must be maintained according to it.

## Purpose / Big Picture

TiDB users need to inspect and archive all persisted table statistics even when the default `lite-init-stats` mode has not loaded histograms, buckets, and TopN values into the current TiDB instance's in-memory statistics cache. After this change, users can query `INFORMATION_SCHEMA.TIDB_STATS_META`, `TIDB_STATS_HISTOGRAMS`, `TIDB_STATS_BUCKETS`, and `TIDB_STATS_TOPN`. The rows come from the statement snapshot of `mysql.stats_*`, are enriched with schema, table, partition, column, and index names from the same snapshot InfoSchema, and retain readable SHOW-compatible values without changing existing `SHOW STATS_*` behavior.

The observable success case is to analyze a table, evict or avoid loading its detailed statistics from the local cache, and still retrieve its persisted histogram, bucket, and TopN rows through the new virtual tables. Predicates on identifying columns must reduce the underlying persistent read, while any predicate the extractor cannot safely consume remains above the MemTable scan for normal SQL evaluation.

## Progress

- [x] (2026-08-11 08:43Z) Read the proposal, repository policy, current branch state, and the change-instruction critic workflow.
- [x] (2026-08-11 08:43Z) Created and switched to `feature/tidb-stats-virtual-tables` without touching pre-existing untracked files.
- [x] (2026-08-11 08:43Z) Located the owning InfoSchema, planner MemTable, executor retriever, statistics decoding, snapshot, and privilege paths.
- [x] (2026-08-11 10:08Z) Registered four Information Schema tables with stable IDs 102 through 105 and the documented column metadata.
- [x] (2026-08-11 10:08Z) Implemented the shared safe predicate extractor for names and numeric identities, including conservative fallback for NULL, parameterized, or non-integral constants.
- [x] (2026-08-11 10:08Z) Implemented snapshot-consistent physical-object mapping, bounded parameterized SQL batches, streaming RecordSet reads, enrichment, decoding, projection, memory tracking, and cleanup.
- [x] (2026-08-11 10:08Z) Added regression coverage for persisted reads after cache eviction, SHOW-compatible values and average sizes, partition/global mappings, predicate behavior, privileges, Bucket cross-batch accumulation, and memory release.
- [x] (2026-08-11 10:08Z) Ran `make bazel_prepare` after the final Go file/import/test-function changes; only `pkg/executor/BUILD.bazel` changed as expected.
- [x] (2026-08-11 10:27Z) Completed the Ready profile: ran source lint with the repository's existing revive 1.2.1 binary, passed the final failpoint-enabled unit and privilege tests, recorded and verified a focused integration test, and completed diff self-review.

## Surprises & Discoveries

- Observation: Current `SHOW STATS_HISTOGRAMS`, `SHOW STATS_BUCKETS`, and `SHOW STATS_TOPN` iterate `StatsHandle` cache entries and therefore cannot establish the proposed full persisted-data contract.
  Evidence: `pkg/executor/show_stats.go` obtains each object through `GetPhysicalTableStats` and skips uninitialized or absent cache objects.

- Observation: InfoSchema virtual table IDs currently end at `autoid.InformationSchemaDBID + 101`.
  Evidence: `pkg/infoschema/tables.go` maps `TableSchemataExtensions` to suffix 101, so the new IDs must be 102 through 105 in append-only order.

- Observation: A streaming source must retain a system session while its `sqlexec.RecordSet` is open. The newer advanced session wrapper guards individual calls but does not wrap later direct calls to the returned RecordSet.
  Evidence: `pkg/session/syssession/session.go` exits its ownership operation immediately after `ExecuteInternal` returns the RecordSet. The existing raw system-session helpers in `pkg/executor/internal/exec/executor.go` retain a session across related operations.

- Observation: Reconstructing a synthetic Histogram solely to call `cardinality.AvgColSize` is unsafe because the synthetic Datum bounds may not match the prepared Chunk layout for the column type.
  Evidence: The first regression run panicked in `Histogram.AppendBucketWithNDV`; copying the small existing formula and using persisted Bucket/TopN totals avoids fabricated bounds and now matches SHOW output in the regression test.

- Observation: Aggregate expressions over persisted TSO and count columns may be returned as DECIMAL Datums even when their inputs are integer columns.
  Evidence: `LAST_ANALYZE_TIME` was initially NULL until the internal SQL explicitly cast the `GREATEST` result to UNSIGNED; Bucket and TopN sums are likewise explicitly cast to SIGNED before typed access.

- Observation: Generic string extraction turns a NULL Datum into an empty string, and generic string-set intersection does not model integer equality coercion for a decimal literal such as `1.0`.
  Evidence: Stats extraction now leaves NULL/parameterized name predicates and non-integral-form numeric predicates for Selection; regressions cover `PARTITION_NAME=NULL`, `IN ('', NULL)`, and combined integer/decimal physical-ID predicates.

- Observation: The Makefile pins the revive installation command to `github.com/mgechev/revive@v1.2.1`, but that module version no longer contains a package at the module root under the current Go toolchain.
  Evidence: Plain `make lint` failed before linting with `module github.com/mgechev/revive@v1.2.1 found, but does not contain package github.com/mgechev/revive`. The repository already had `tools/bin/revive` version 1.2.1, so `make lint GO=true` skipped only the broken phony reinstall and successfully ran both revive invocations and every dashboard linter command.

- Observation: The classic unistore integration-test server does not seed `mysql.tidb.tikv_gc_safe_point`, while setting `tidb_snapshot` requires it.
  Evidence: The first focused integration recording failed with `can not get 'tikv_gc_safe_point'`; initializing the test-only safe point, as the mock-store regression already does, made recording and non-recording verification pass.

## Decision Log

- Decision: Implement proposal option one, four new `INFORMATION_SCHEMA.TIDB_STATS_*` tables, and do not extend SHOW grammar.
  Rationale: The proposal explicitly marks SHOW syntax extension as abandoned. Virtual tables are composable for history collection and preserve existing SHOW compatibility.
  Date/Author: 2026-08-11 / Codex

- Decision: Use a specialized stateful MemTable retriever backed by a streaming `sqlexec.RecordSet`, not the generic one-shot MemTable retriever and not forced StatsCache loading.
  Rationale: A single histogram may contain 100,000 large Bucket or TopN rows. One-shot materialization violates the memory goal, while forced cache loading mutates instance state and does not naturally share one statement snapshot across persistent data and names.
  Date/Author: 2026-08-11 / Codex

- Decision: Resolve all physical table and histogram identities from the snapshot InfoSchema before generating internal SQL; ignore persistent rows whose identifiers cannot be resolved at that snapshot.
  Rationale: Names and types must be from the same visibility point as persistent statistics. Unresolvable rows are stale or outside the statement schema snapshot and cannot be enriched correctly.
  Date/Author: 2026-08-11 / Codex

- Decision: Keep unsupported or unsafe predicates in the upper Selection and only remove exact constraints that the retriever enforces.
  Rationale: This preserves SQL three-valued logic and collation semantics while still allowing useful `=`, `IN`, and safely evaluated `LIKE` pruning.
  Date/Author: 2026-08-11 / Codex

- Decision: Retain the raw system session for the entire RecordSet lifecycle and destroy it after any query, iteration, or cancellation error.
  Rationale: RecordSet iteration outlives `ExecuteInternal`; an unhealthy or canceled session should not be returned to the shared pool even if clearing `tidb_snapshot` succeeds.
  Date/Author: 2026-08-11 / Codex

- Decision: Compute SHOW-equivalent average column size directly from persisted row, Bucket, TopN, NULL, and total-column-size counts.
  Rationale: The formula is small and stable, while synthesizing a type-correct Histogram only to call it introduced invalid Chunk layouts and unnecessary allocations.
  Date/Author: 2026-08-11 / Codex

## Outcomes & Retrospective

The four persistent statistics virtual tables are implemented on `feature/tidb-stats-virtual-tables`. They read `mysql.stats_*` at the statement timestamp without loading detailed statistics into StatsCache, use names and types from the matching snapshot InfoSchema, preserve SHOW-compatible Bucket, TopN, and average-column-size output, and expose exact privilege checks and conservative predicate pushdown.

The targeted regression proves that detailed SHOW output becomes empty after StatsCache is cleared while the new virtual tables continue returning the persisted rows. It also compares Bucket, TopN, and average-size values directly with SHOW output before eviction, covers non-partitioned/global/partition identity mapping and extraction edge cases, verifies Bucket cumulative counts across executor batches and complete tracker release, and checks the four-table privilege matrix. A focused classic integration test additionally proves all four SQL surfaces against a built TiDB server.

Ready validation passed. No production behavior gaps remain within the proposal. Real TiKV and broad package or repository sweeps were not run because the implementation reads the existing SQL system tables and the repository policy calls for the smallest relevant validation set; the focused integration test used classic unistore.

## Context and Orientation

`pkg/infoschema/tables.go` defines Information Schema table names, stable table IDs, and column metadata. `pkg/planner/core/logical_plan_builder.go` attaches a `MemTablePredicateExtractor` when a query scans a supported virtual table. Extractors live in `pkg/planner/core/memtable_predicate_extractor.go`; an extractor may consume a predicate only when the executor fully enforces it, otherwise the predicate remains for an upper `Selection` operator.

`pkg/executor/builder.go` routes a physical MemTable to a `memTableRetriever`. `pkg/executor/memtable_reader.go` defines the retriever lifecycle: repeated `retrieve` calls return row batches and `close` must release resources. `pkg/executor/show_stats.go` contains the existing user-facing time conversion and SHOW-compatible Bucket and TopN formatting. `pkg/statistics/handle/storage/read.go` contains the persisted bound conversion rules used when loading column histograms.

A physical table ID is the key in `mysql.stats_*`. For a non-partitioned table it equals the logical table ID. For a partitioned table, the logical table ID denotes global statistics and each partition definition ID denotes partition statistics. A histogram identity is `(physical_id, is_index, hist_id)`, where `hist_id` is a column ID when `is_index=0` and an index ID when `is_index=1`.

The statement read timestamp, abbreviated read TS, is the TiKV timestamp used by the current SQL statement. Snapshot InfoSchema is the schema metadata visible at that timestamp. The retriever must obtain both through `sessiontxn.GetTxnManager(sctx).GetStmtReadTS()` and `domain.GetDomain(sctx).GetSnapshotInfoSchema(readTS)`, then set that read TS as `tidb_snapshot` in its retained internal session before reading `mysql.stats_*`.

## Plan of Work

First extend `pkg/infoschema/tables.go` with exported constants for the four table names, IDs 102 through 105, and the exact types, nullability, unsigned flags, and sizes described by the proposal. Add the four name-to-column mappings so normal InfoSchema bootstrap exposes them.

Next add a shared `StatsTableExtractor` in the planner. It will record exact string and integer filters plus safe LIKE patterns for the identifying columns present in each table. Its `Extract` method will consume only predicates whose semantics are preserved by later snapshot-object matching, set `SkipRequest` for contradictory predicates, and leave all other expressions untouched. Attach it to all four tables in `buildMemTable` and add extractor tests, including explain output and retained unsupported conditions.

Then route all four physical MemTables in `pkg/executor/builder.go` to a new stateful retriever. The retriever will establish the read TS and snapshot InfoSchema, verify the backing-table privilege corresponding to the selected virtual table, construct stable physical-object metadata, and open a source owning one internal system session. The source will set `tidb_snapshot`, split resolved identities into bounded query batches, issue parameterized SQL without concatenating user values, and keep only one RecordSet and one source Chunk live at a time.

The source returns cloned Datum rows in batches bounded by row count and estimated bytes. The retriever maps identifiers to names and types, converts TSO versions to DATETIME, computes last analyze time, decodes column bounds with the persisted storage conversion rules, formats index encodings with `statistics.ValueToString`, maintains cumulative Bucket counts across return batches, calculates histogram average column size with SHOW-equivalent rules, projects requested output columns, and accounts for retained row memory. `close` must close any RecordSet, clear `tidb_snapshot`, rollback or discard an unhealthy internal session, return healthy sessions to the pool, and release the last returned batch's accounting.

Finally extend existing planner and executor test files rather than adding parallel scaffolding where practical. Tests will prove persisted rows remain visible independently of local detailed-cache loading, verify non-partitioned/global/partition names and IDs, cover column and composite-index readable values, ensure Bucket counts remain cumulative across deliberately small batches, check exact and LIKE pruning without changing query results, and validate the documented privileges. Because imports and top-level test functions or Go files will change, run the Bazel preparation gate and retain only required generated metadata.

## Concrete Steps

Run all commands from `/Users/c113/code/tidb`.

During implementation, format changed Go files and run scoped compile/tests using the WIP verification profile. Before any test command, inspect package failpoint usage with the failpoint test runner skill.

    gofmt -w <changed-go-files>
    make bazel_prepare
    go test ./pkg/planner/core/operator/logicalop/logicalop_test -run '<target extractor test>'
    go test ./pkg/executor -run '<target stats virtual table test>'
    go test ./pkg/executor/test/showtest -run '<target privilege test>'

The final targeted invocations are:

    ./tools/check/failpoint-go-test.sh pkg/executor \
      -run '^(TestTiDBStatsVirtualTablesReadPersistentStats|TestStatsMemTableBucketCountAcrossBatches)$' \
      -count=1
    ./tools/check/failpoint-go-test.sh pkg/executor/test/showtest \
      -run '^TestShowStatsPrivilege$' -count=1
    cd tests/integrationtest
    ./run-tests.sh -r statistics/tidb_stats_virtual_tables
    ./run-tests.sh -s ./integrationtest_tidb-server -t statistics/tidb_stats_virtual_tables

`make bazel_lint_changed` must not be run because repository policy reserves it for explicit requests.

For Ready validation after code changes, plain `make lint` currently fails in its phony revive installation prerequisite as recorded under `Surprises & Discoveries`. With the existing matching revive binary, run the otherwise identical target as:

    make lint GO=true

Then rerun all targeted regression commands and inspect:

    git status --short
    git diff --check
    git diff --stat
    git diff -- <changed paths>

Expected results are zero exit status, no unexpected generated churn, and only files needed for these virtual tables and their tests.

## Validation and Acceptance

Create a table with scalar columns, a composite index, and enough skew for TopN and Buckets, then analyze it. Query each new virtual table with `TABLE_SCHEMA='test'` and `TABLE_NAME=<name>`. The META row must show logical and physical IDs, names, counts, update time, and a nullable last analyze time. HISTOGRAMS must show both column and index objects with persisted NDV/null/count metadata. BUCKETS and TOPN must show readable values matching existing SHOW formatting, not hex blobs.

For a partitioned table analyzed with dynamic pruning, the logical table ID row must be named `global`, while definition IDs use their partition names. A non-partitioned table uses an empty partition name. Bucket output ordered by histogram identity and persisted bucket ID must expose cumulative counts even when a histogram crosses retriever batches.

Run `EXPLAIN` and result queries using `=`, `IN`, and LIKE filters on supported columns. Explain output must report extracted constraints, source SQL must be bounded to resolved IDs, and final rows must equal normal SQL semantics. Unsupported expressions remain as Selection conditions.

Authenticate users with no stats privileges, one underlying stats-table privilege, and `SELECT ON mysql.*`. Access must follow the proposal: META requires `mysql.stats_meta`, HISTOGRAMS requires `mysql.stats_histograms`, BUCKETS requires `mysql.stats_buckets`, and TOPN requires database-level mysql SELECT.

## Idempotence and Recovery

Formatting, Bazel preparation, and targeted tests are safe to rerun. Existing untracked files predate this task and must never be deleted or staged as part of recovery. If an internal SQL read fails, the retriever closes the RecordSet, clears or abandons the internal session, and releases accounted memory before returning the error; rerunning the user query starts from a fresh retriever and statement timestamp.

If generated Bazel metadata includes unrelated changes, inspect ownership before editing and retain user changes. Do not reset the worktree. If a stable InfoSchema ID collision appears after an upstream update, append after the new highest ID and record the change in this plan rather than renumbering any existing table.

## Artifacts and Notes

Branch creation evidence:

    Switched to a new branch 'feature/tidb-stats-virtual-tables'

Initial repository state contained only pre-existing untracked files on `master`; no tracked modifications were present before this plan.

Final focused validation evidence:

    PASS
    ok github.com/pingcap/tidb/pkg/executor
    PASS
    ok github.com/pingcap/tidb/pkg/executor/test/showtest
    ./t/statistics/tidb_stats_virtual_tables.test: ok! 10 test cases passed
    integrationtest passed!

Both failpoint-enabled runs ended with `new_refcount=0`. The generated Bazel change is limited to adding `stats_memtable.go` and its direct `ngaut/pools` dependency to `pkg/executor/BUILD.bazel`.

## Interfaces and Dependencies

Define `plannercore.StatsTableExtractor` as the common planner-side constraint carrier for all four tables. It must implement `base.MemTablePredicateExtractor` through:

    Extract(ctx base.PlanContext, schema *expression.Schema, names []*types.FieldName, predicates []expression.Expression) []expression.Expression
    ExplainInfo(plan base.PhysicalPlan) string

Define an executor-side `statsMemTableRetriever` implementing the existing unexported `memTableRetriever` contract:

    retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error)
    close() error
    getRuntimeStats() execdetails.RuntimeStats

Define an internal source interface whose ownership contract is explicit:

    Open(ctx context.Context, sctx sessionctx.Context, readTS uint64, queries []statsQuery) error
    NextBatch(ctx context.Context, maxRows int, maxBytes int64) (rows [][]types.Datum, done bool, err error)
    Close() error

The implementation uses `sessiontxn` for the current statement timestamp, `domain` and `infoschema.InfoSchema` for snapshot metadata, `sqlexec.RecordSet` and `chunk.Chunk` for streaming internal SQL, `statistics.ValueToString` for SHOW-compatible encoded index formatting, the stats storage bound conversion contract for scalar values, `oracle.GetTimeFromTS` for DATETIME conversion, and statement memory trackers from `pkg/util/memory`.

Plan revision note (2026-08-11): Initial plan created after repository evidence review and before source edits. Updated after implementation to record final design discoveries, exact Ready validation commands, focused integration coverage, outcomes, and remaining validation boundaries.
