// Copyright 2024 PingCAP, Inc.
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

package pushdowntest

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

// TestBitCastInTiKV see issue: https://github.com/pingcap/tidb/issues/56494
func TestBitCastInTiKV(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	defer tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a bit(24))")
	tk.MustExec("insert into t1 values(0xffffff)")
	err := tk.QueryToErr("select a from t1 where false not like convert(a, char)")
	require.EqualError(t, err, "[tikv:3854]Cannot convert string '\\xFF\\xFF\\xFF' from binary to utf8mb4")
}

// TestLogicalShortCircuitPushDown verifies that TiDB and TiKV honor logical
// short-circuit evaluation consistently. See https://github.com/tikv/tikv/issues/19801.
func TestLogicalShortCircuitPushDown(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_logical_short_circuit")
	defer tk.MustExec("drop table if exists t_logical_short_circuit")
	tk.MustExec(`create table t_logical_short_circuit (
		id int primary key,
		or_lhs int,
		and_lhs int,
		rhs varchar(32)
	)`)
	tk.MustExec(`insert into t_logical_short_circuit values
		(1, 1, 0, 'invalid-int'),
		(2, 0, 1, '2'),
		(3, null, null, '0'),
		(4, null, null, '1'),
		(5, 0, 1, '0')`)

	originalShortCircuitSetting := tk.MustQuery(
		"select @@session.tidb_enable_short_circuit_expression",
	).Rows()[0][0].(string)
	defer func() {
		tk.MustExec("set @@session.tidb_enable_short_circuit_expression = " + originalShortCircuitSetting)
	}()
	originalVectorizedSetting := tk.MustQuery(
		"select @@session.tidb_enable_vectorized_expression",
	).Rows()[0][0].(string)
	defer func() {
		tk.MustExec("set @@session.tidb_enable_vectorized_expression = " + originalVectorizedSetting)
	}()
	tk.MustExec("set @@session.tidb_enable_vectorized_expression = on")

	requirePlanOperator := func(sql, operator, task string, details ...string) {
		plan := tk.MustQuery("explain format = 'brief' " + sql)
		found := false
		for _, row := range plan.Rows() {
			if len(row) < 3 || !strings.Contains(row[0].(string), operator) || row[2].(string) != task {
				continue
			}
			var rowText strings.Builder
			for _, column := range row {
				rowText.WriteString(column.(string))
			}
			matchesDetails := true
			for _, detail := range details {
				if !strings.Contains(rowText.String(), detail) {
					matchesDetails = false
					break
				}
			}
			if matchesDetails {
				found = true
				break
			}
		}
		require.Truef(t, found, "expected %s containing %v to run in %s, plan:\n%s", operator, details, task, plan.String())
	}

	logicalQueries := []struct {
		name     string
		sql      string
		expected [][]any
		operator string
		details  []string
	}{
		{
			name:     "OR",
			sql:      "select id from t_logical_short_circuit where or_lhs or cast(rhs as signed) order by id",
			expected: testkit.Rows("1", "2", "4"),
			operator: "Selection",
			details:  []string{"or(", "cast("},
		},
		{
			name:     "AND",
			sql:      "select id from t_logical_short_circuit where (and_lhs and cast(rhs as signed)) is null order by id",
			expected: testkit.Rows("4"),
			operator: "Selection",
			details:  []string{"and(", "cast("},
		},
	}
	for _, query := range logicalQueries {
		requirePlanOperator(query.sql, query.operator, "cop[tikv]", query.details...)
	}

	legacyWarning := testkit.Rows("Warning 1292 evaluation failed: Truncated incorrect INTEGER value: 'invalid-int'")
	tk.MustExec("set @@session.tidb_enable_short_circuit_expression = off")
	for _, query := range logicalQueries {
		result := tk.MustQuery(query.sql)
		result.AddComment(query.name + " with short-circuit evaluation disabled")
		result.Check(query.expected)
		warnings := tk.MustQuery("show warnings")
		warnings.AddComment(query.name + " with short-circuit evaluation disabled")
		warnings.Check(legacyWarning)
	}

	tk.MustExec("set @@session.tidb_enable_short_circuit_expression = on")
	settingBeforeHint := tk.MustQuery(
		"select @@session.tidb_enable_short_circuit_expression",
	).Rows()[0][0].(string)
	for _, query := range logicalQueries {
		result := tk.MustQuery(query.sql)
		result.AddComment(query.name + " with short-circuit evaluation enabled")
		result.Check(query.expected)
		warnings := tk.MustQuery("show warnings")
		warnings.AddComment(query.name + " with short-circuit evaluation enabled")
		warnings.Check(testkit.Rows())
	}

	// SET_VAR is applied after the statement context is reset. This verifies that
	// it updates the current DAG flag and is restored for the following statement.
	hintOffSQL := `select /*+ set_var(tidb_enable_short_circuit_expression=off) */
		id from t_logical_short_circuit where or_lhs or cast(rhs as signed) order by id`
	requirePlanOperator(hintOffSQL, "Selection", "cop[tikv]", "or(", "cast(")
	tk.MustQuery(hintOffSQL).Check(testkit.Rows("1", "2", "4"))
	tk.MustQuery("show warnings").Check(legacyWarning)
	tk.MustQuery("select @@session.tidb_enable_short_circuit_expression").Check(testkit.Rows(settingBeforeHint))
	tk.MustQuery(logicalQueries[0].sql).Check(logicalQueries[0].expected)
	tk.MustQuery("show warnings").Check(testkit.Rows())

	projectionResult := testkit.Rows(
		"1 1 0",
		"2 1 1",
		"3 <nil> 0",
		"4 1 <nil>",
		"5 0 0",
	)
	rootProjectionSQL := `select /*+ set_var(tidb_opt_projection_push_down=off) */
		id, or_lhs or cast(rhs as signed), and_lhs and cast(rhs as signed)
		from t_logical_short_circuit order by id`
	requirePlanOperator(rootProjectionSQL, "Projection", "root", "or(", "and(", "cast(")
	tk.MustQuery(rootProjectionSQL).Check(projectionResult)
	tk.MustQuery("show warnings").Check(testkit.Rows())

	// Projection elimination replaces r with RAND after the OR expression is
	// built. The first RAND(1) value is below 0.5, so the RHS is required: its
	// warning must be preserved and the mutable LHS must not be evaluated again.
	rootMutableSQL := `select /*+ set_var(tidb_opt_projection_push_down=off) */
		r or cast(rhs as signed)
		from (
			select rand(1) > 0.5 as r, rhs
			from t_logical_short_circuit where id = 1
		) d`
	requirePlanOperator(rootMutableSQL, "Projection", "root", "or(", "rand(", "cast(")
	tk.MustQuery(rootMutableSQL).Check(testkit.Rows("0"))
	rootWarnings := tk.MustQuery("show warnings").Rows()
	require.Len(t, rootWarnings, 1)
	require.Contains(t, rootWarnings[0][2].(string), "Truncated incorrect INTEGER value: 'invalid-int'")

	orTruthQueries := []struct {
		sql      string
		expected [][]any
	}{
		{
			sql:      "select id from t_logical_short_circuit where (or_lhs or cast(rhs as signed)) is true order by id",
			expected: testkit.Rows("1", "2", "4"),
		},
		{
			sql:      "select id from t_logical_short_circuit where (or_lhs or cast(rhs as signed)) is false order by id",
			expected: testkit.Rows("5"),
		},
		{
			sql:      "select id from t_logical_short_circuit where (or_lhs or cast(rhs as signed)) is null order by id",
			expected: testkit.Rows("3"),
		},
	}
	for _, query := range orTruthQueries {
		requirePlanOperator(query.sql, "Selection", "cop[tikv]", "or(", "cast(")
		tk.MustQuery(query.sql).Check(query.expected)
		tk.MustQuery("show warnings").Check(testkit.Rows())
	}

	// A pushed-down wrapper keeps AND as one TiKV expression instead of letting
	// the planner split a top-level AND into separate CNF predicates.
	andTruthQueries := []struct {
		sql      string
		expected [][]any
	}{
		{
			sql:      "select id from t_logical_short_circuit where (and_lhs and cast(rhs as signed)) is true order by id",
			expected: testkit.Rows("2"),
		},
		{
			sql:      "select id from t_logical_short_circuit where (and_lhs and cast(rhs as signed)) is false order by id",
			expected: testkit.Rows("1", "3", "5"),
		},
		{
			sql:      logicalQueries[1].sql,
			expected: logicalQueries[1].expected,
		},
	}
	for _, query := range andTruthQueries {
		requirePlanOperator(query.sql, "Selection", "cop[tikv]", "and(", "cast(")
		tk.MustQuery(query.sql).Check(query.expected)
		tk.MustQuery("show warnings").Check(testkit.Rows())
	}
}
