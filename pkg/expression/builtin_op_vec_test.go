// Copyright 2019 PingCAP, Inc.
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

package expression

import (
	"math"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

var vecBuiltinOpCases = map[string][]vecExprBenchCase{
	ast.IsTruthWithoutNull: {
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETReal}},
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETDecimal}},
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETInt}},
	},
	ast.IsFalsity: {
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETReal}},
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETDecimal}},
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETInt}},
	},
	ast.LogicOr: {
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, geners: makeBinaryLogicOpDataGeners()},
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETDecimal, types.ETReal}},
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETInt, types.ETDuration}},
	},
	ast.LogicXor: {
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, geners: makeBinaryLogicOpDataGeners()},
	},
	ast.Xor: {
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, geners: makeBinaryLogicOpDataGeners()},
	},
	ast.LogicAnd: {
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, geners: makeBinaryLogicOpDataGeners()},
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETDecimal, types.ETReal}},
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETInt, types.ETDuration}},
	},
	ast.Or: {
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, geners: makeBinaryLogicOpDataGeners()},
	},
	ast.BitNeg: {
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETInt}},
	},
	ast.UnaryNot: {
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETReal}},
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETDecimal}},
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETInt}},
	},
	ast.And: {
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, geners: makeBinaryLogicOpDataGeners()},
	},
	ast.RightShift: {
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETInt, types.ETInt}},
	},
	ast.LeftShift: {
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETInt, types.ETInt}},
	},
	ast.UnaryMinus: {
		{retEvalType: types.ETReal, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETReal}},
		{retEvalType: types.ETDecimal, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETDecimal}},
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETInt}},
		{
			retEvalType:   types.ETInt,
			aesModes:      "aes-128-ecb",
			childrenTypes: []types.EvalType{types.ETInt},
			childrenFieldTypes: []*types.FieldType{
				types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).SetFlag(mysql.UnsignedFlag).BuildP(),
			},
			geners: []dataGenerator{newRangeInt64Gener(0, math.MaxInt64)},
		},
	},
	ast.IsNull: {
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETReal}},
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETInt}},
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETDecimal}},
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETDuration}},
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETDatetime}},
	},
}

// givenValsGener returns the items sequentially from the slice given at
// the construction time. If this slice is exhausted, it falls back to
// the fallback generator.
type givenValsGener struct {
	given    []any
	idx      int
	fallback dataGenerator
}

func (g *givenValsGener) gen() any {
	if g.idx >= len(g.given) {
		return g.fallback.gen()
	}
	v := g.given[g.idx]
	g.idx++
	return v
}

func makeGivenValsOrDefaultGener(vals []any, eType types.EvalType) *givenValsGener {
	g := &givenValsGener{}
	g.given = vals
	g.fallback = newDefaultGener(0.2, eType)
	return g
}

func makeBinaryLogicOpDataGeners() []dataGenerator {
	// TODO: rename this to makeBinaryOpDataGenerator, since the BIT ops are also using it?
	pairs := [][]any{
		{nil, nil},
		{0, nil},
		{nil, 0},
		{1, nil},
		{nil, 1},
		{0, 0},
		{0, 1},
		{1, 0},
		{1, 1},
		{-1, 1},
	}

	maybeToInt64 := func(v any) any {
		if v == nil {
			return nil
		}
		return int64(v.(int))
	}

	n := len(pairs)
	arg0s := make([]any, n)
	arg1s := make([]any, n)
	for i, p := range pairs {
		arg0s[i] = maybeToInt64(p[0])
		arg1s[i] = maybeToInt64(p[1])
	}
	return []dataGenerator{
		makeGivenValsOrDefaultGener(arg0s, types.ETInt),
		makeGivenValsOrDefaultGener(arg1s, types.ETInt)}
}

func TestVectorizedBuiltinOpFunc(t *testing.T) {
	testVectorizedBuiltinFunc(t, vecBuiltinOpCases)
	testVectorizedLogicOpShortCircuit(t)
}

type logicOpTestValue struct {
	value  int64
	isNull bool
}

type logicOpTestRow struct {
	lhs logicOpTestValue
	rhs logicOpTestValue
}

type selectionRecordingExpr struct {
	Expression
	selections [][]int
	err        error
}

func (e *selectionRecordingExpr) VecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	e.selections = append(e.selections, append([]int(nil), input.Sel()...))
	if e.err != nil {
		return e.err
	}
	return e.Expression.VecEvalInt(ctx, input, result)
}

func (e *selectionRecordingExpr) EvalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	if e.err != nil {
		return 0, true, e.err
	}
	return e.Expression.EvalInt(ctx, row)
}

func newLogicOpTestInput(rows []logicOpTestRow, sel []int) (*chunk.Chunk, *Column, *Column) {
	physicalRows := len(rows)
	rowsByPhysicalIndex := rows
	if sel != nil {
		physicalRows = 0
		for _, physicalRow := range sel {
			physicalRows = max(physicalRows, physicalRow+1)
		}
		rowsByPhysicalIndex = make([]logicOpTestRow, physicalRows)
		for logicalRow, physicalRow := range sel {
			rowsByPhysicalIndex[physicalRow] = rows[logicalRow]
		}
	}

	fts := []*types.FieldType{eType2FieldType(types.ETInt), eType2FieldType(types.ETInt)}
	input := chunk.NewChunkWithCapacity(fts, physicalRows)
	for _, row := range rowsByPhysicalIndex {
		if row.lhs.isNull {
			input.AppendNull(0)
		} else {
			input.AppendInt64(0, row.lhs.value)
		}
		if row.rhs.isNull {
			input.AppendNull(1)
		} else {
			input.AppendInt64(1, row.rhs.value)
		}
	}
	input.SetSel(sel)
	return input,
		&Column{Index: 0, RetType: fts[0]},
		&Column{Index: 1, RetType: fts[1]}
}

func testVectorizedLogicOp(
	t *testing.T,
	funcName string,
	shortCircuitEnabled bool,
	rows []logicOpTestRow,
	sel []int,
	expectedRHSSelections [][]int,
	expected []logicOpTestValue,
) {
	ctx := mock.NewContext()
	shortCircuitSetting := "OFF"
	if shortCircuitEnabled {
		shortCircuitSetting = "ON"
	}
	require.NoError(t, ctx.GetSessionVars().SetSystemVar("tidb_enable_short_circuit_expression", shortCircuitSetting))
	originalSel := append([]int(nil), sel...)
	input, lhs, rhs := newLogicOpTestInput(rows, sel)
	recordedRHS := &selectionRecordingExpr{Expression: rhs}
	f, err := funcs[funcName].getFunction(ctx, []Expression{lhs, recordedRHS})
	require.NoError(t, err)
	require.True(t, f.vectorized() && f.isChildrenVectorized())

	result := chunk.NewColumn(eType2FieldType(types.ETInt), len(rows))
	require.NoError(t, f.vecEvalInt(ctx, input, result))
	require.Equal(t, originalSel, input.Sel())
	require.Equal(t, expectedRHSSelections, recordedRHS.selections)
	require.Equal(t, len(expected), getColumnLen(result, types.ETInt))

	values := result.Int64s()
	for i, expectedValue := range expected {
		require.Equal(t, expectedValue.isNull, result.IsNull(i))
		if !expectedValue.isNull {
			require.Equal(t, expectedValue.value, values[i])
		}
	}
}

func testVectorizedLogicOpShortCircuit(t *testing.T) {
	null := logicOpTestValue{isNull: true}

	// The original selection is reordered and does not cover every physical row.
	// Logical OR should pass only rows whose LHS is not true to the RHS.
	orRows := []logicOpTestRow{
		{lhs: logicOpTestValue{value: 2}, rhs: logicOpTestValue{}},
		{lhs: logicOpTestValue{}, rhs: logicOpTestValue{value: 1}},
		{lhs: null, rhs: logicOpTestValue{value: 1}},
		{lhs: null, rhs: logicOpTestValue{}},
		{lhs: null, rhs: null},
		{lhs: logicOpTestValue{}, rhs: null},
		{lhs: logicOpTestValue{}, rhs: logicOpTestValue{}},
	}
	orSel := []int{7, 2, 8, 1, 6, 3, 5}
	testVectorizedLogicOp(t, ast.LogicOr, true, orRows, orSel,
		[][]int{{2, 8, 1, 6, 3, 5}},
		[]logicOpTestValue{
			{value: 1}, // A nonzero LHS is normalized to TRUE without evaluating RHS.
			{value: 1},
			{value: 1},
			null,
			null,
			null,
			{},
		})

	// Logical AND should pass only rows whose LHS is not false to the RHS.
	andRows := []logicOpTestRow{
		{lhs: logicOpTestValue{}, rhs: logicOpTestValue{value: 1}},
		{lhs: logicOpTestValue{value: 2}, rhs: logicOpTestValue{value: 3}},
		{lhs: null, rhs: logicOpTestValue{}},
		{lhs: null, rhs: logicOpTestValue{value: 1}},
		{lhs: null, rhs: null},
		{lhs: logicOpTestValue{value: 1}, rhs: null},
		{lhs: logicOpTestValue{value: 1}, rhs: logicOpTestValue{}},
	}
	andSel := []int{6, 1, 8, 2, 7, 3, 5}
	testVectorizedLogicOp(t, ast.LogicAnd, true, andRows, andSel,
		[][]int{{1, 8, 2, 7, 3, 5}},
		[]logicOpTestValue{
			{},
			{value: 1},
			{},
			null,
			null,
			null,
			{},
		})

	// Disabling short-circuit evaluation preserves the legacy eager RHS path.
	testVectorizedLogicOp(t, ast.LogicOr, false, orRows, orSel,
		[][]int{orSel},
		[]logicOpTestValue{{value: 1}, {value: 1}, {value: 1}, null, null, null, {}})
	testVectorizedLogicOp(t, ast.LogicAnd, false, andRows, andSel,
		[][]int{andSel},
		[]logicOpTestValue{{}, {value: 1}, {}, null, null, null, {}})

	// Exercise both short-circuit fast paths and a chunk without an original selection.
	testVectorizedLogicOp(t, ast.LogicOr, true,
		[]logicOpTestRow{
			{lhs: logicOpTestValue{value: 2}},
			{lhs: logicOpTestValue{value: -1}},
		},
		[]int{3, 1}, nil,
		[]logicOpTestValue{{value: 1}, {value: 1}})
	testVectorizedLogicOp(t, ast.LogicOr, true,
		[]logicOpTestRow{
			{rhs: logicOpTestValue{value: 1}},
			{lhs: null, rhs: logicOpTestValue{}},
		},
		[]int{2, 0}, [][]int{{2, 0}},
		[]logicOpTestValue{{value: 1}, null})
	testVectorizedLogicOp(t, ast.LogicOr, true,
		[]logicOpTestRow{
			{lhs: logicOpTestValue{value: 1}},
			{rhs: logicOpTestValue{value: 1}},
		},
		nil, [][]int{{1}},
		[]logicOpTestValue{{value: 1}, {value: 1}})
	testVectorizedLogicOp(t, ast.LogicAnd, true,
		[]logicOpTestRow{
			{lhs: logicOpTestValue{}},
			{lhs: logicOpTestValue{}},
		},
		[]int{3, 1}, nil,
		[]logicOpTestValue{{}, {}})
	testVectorizedLogicOp(t, ast.LogicAnd, true,
		[]logicOpTestRow{
			{lhs: logicOpTestValue{value: 1}, rhs: logicOpTestValue{value: 2}},
			{lhs: null, rhs: logicOpTestValue{}},
		},
		[]int{2, 0}, [][]int{{2, 0}},
		[]logicOpTestValue{{value: 1}, {}})
	testVectorizedLogicOp(t, ast.LogicAnd, true,
		[]logicOpTestRow{
			{lhs: logicOpTestValue{}},
			{lhs: logicOpTestValue{value: 1}, rhs: logicOpTestValue{value: 1}},
		},
		nil, [][]int{{1}},
		[]logicOpTestValue{{}, {value: 1}})

	testVectorizedLogicOpRestoresSelectionOnError(t, ast.LogicOr,
		[]logicOpTestRow{
			{lhs: logicOpTestValue{value: 1}},
			{lhs: logicOpTestValue{}},
			{lhs: null},
		},
		[]int{4, 1, 3}, []int{1, 3})
	testVectorizedLogicOpRestoresSelectionOnError(t, ast.LogicAnd,
		[]logicOpTestRow{
			{lhs: logicOpTestValue{}},
			{lhs: logicOpTestValue{value: 1}},
			{lhs: null},
		},
		[]int{4, 1, 3}, []int{1, 3})
}

func testVectorizedLogicOpRestoresSelectionOnError(
	t *testing.T,
	funcName string,
	rows []logicOpTestRow,
	sel []int,
	expectedRHSSelection []int,
) {
	ctx := mock.NewContext()
	require.NoError(t, ctx.GetSessionVars().SetSystemVar("tidb_enable_short_circuit_expression", "ON"))
	originalSel := append([]int(nil), sel...)
	input, lhs, rhs := newLogicOpTestInput(rows, sel)
	recordedRHS := &selectionRecordingExpr{
		Expression: rhs,
		err:        types.ErrOverflow.GenWithStackByArgs("BIGINT", "short circuit test"),
	}
	f, err := funcs[funcName].getFunction(ctx, []Expression{lhs, recordedRHS})
	require.NoError(t, err)
	require.True(t, f.vectorized() && f.isChildrenVectorized())

	result := chunk.NewColumn(eType2FieldType(types.ETInt), len(rows))
	require.Error(t, f.vecEvalInt(ctx, input, result))
	require.Equal(t, originalSel, input.Sel())
	require.Equal(t, [][]int{expectedRHSSelection}, recordedRHS.selections)
}

func BenchmarkVectorizedBuiltinOpFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinOpCases)
}

func TestBuiltinUnaryMinusIntSig(t *testing.T) {
	ctx := mock.NewContext()
	ft := eType2FieldType(types.ETInt)
	col0 := &Column{RetType: ft, Index: 0}
	f, err := funcs[ast.UnaryMinus].getFunction(ctx, []Expression{col0})
	require.NoError(t, err)
	require.True(t, f.vectorized() && f.isChildrenVectorized())
	input := chunk.NewChunkWithCapacity([]*types.FieldType{ft}, 1024)
	result := chunk.NewColumn(ft, 1024)

	require.False(t, mysql.HasUnsignedFlag(col0.GetType(ctx).GetFlag()))
	input.AppendInt64(0, 233333)
	require.NoError(t, vecEvalType(ctx, f, types.ETInt, input, result))
	require.Equal(t, int64(-233333), result.GetInt64(0))
	input.Reset()
	input.AppendInt64(0, math.MinInt64)
	require.Error(t, vecEvalType(ctx, f, types.ETInt, input, result))
	input.Column(0).SetNull(0, true)
	require.NoError(t, vecEvalType(ctx, f, types.ETInt, input, result))
	require.True(t, result.IsNull(0))

	col0.GetType(ctx).AddFlag(mysql.UnsignedFlag)
	require.True(t, mysql.HasUnsignedFlag(col0.GetType(ctx).GetFlag()))
	input.Reset()
	input.AppendUint64(0, 233333)
	require.NoError(t, vecEvalType(ctx, f, types.ETInt, input, result))
	require.Equal(t, int64(-233333), result.GetInt64(0))
	input.Reset()
	input.AppendUint64(0, -(math.MinInt64)+1)
	require.Error(t, vecEvalType(ctx, f, types.ETInt, input, result))
	input.Column(0).SetNull(0, true)
	require.NoError(t, vecEvalType(ctx, f, types.ETInt, input, result))
	require.True(t, result.IsNull(0))
}
