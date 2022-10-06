/*
 * Copyright 2022 CECTC, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cond

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cectc/dbpack/pkg/topo"
	"github.com/cectc/dbpack/pkg/visitor"
	"github.com/cectc/dbpack/third_party/parser"
	"github.com/cectc/dbpack/third_party/parser/ast"
)

func TestNumberModShard(t *testing.T) {
	var shardAlg = &NumberMod{
		shardingKey: "uid",
		topology:    mockTopology(),
	}

	testCases := []*struct {
		sql  string
		args []interface{}
	}{
		{
			sql:  "select * from student where 1 != 1",
			args: []interface{}{5, 18},
		},
		{
			sql:  "select * from student where ? = uid or uid = ?",
			args: []interface{}{5, 8},
		},
		{
			sql:  "select * from student where uid = ? or age = ?",
			args: []interface{}{5, 18},
		},
		{
			// uid = 5 or ((age = 18 and name = jane) or (age = 18 and gender = 1))
			sql:  "select * from student where uid = ? or (age = ? and (name = ? or gender = ?))",
			args: []interface{}{5, 18, "jane", 1},
		},
		{
			sql:  "select * from student where uid = ? and (age = ? or name = ?)",
			args: []interface{}{5, 18, "jane"},
		},
		{
			sql:  "select * from student where uid > ? and uid < ? and age = ?",
			args: []interface{}{5, 80, 18},
		},
		{
			sql:  "select * from student where uid > ? and uid < ? and age = ?",
			args: []interface{}{80, 5, 18},
		},
	}

	p := parser.New()
	for _, c := range testCases {
		t.Run(c.sql, func(t *testing.T) {
			stmt, err := p.ParseOneStmt(c.sql, "", "")
			if assert.NoErrorf(t, err, "parse sql statement err: %s", err) {
				stmt.Accept(&visitor.ParamVisitor{})
				sel := stmt.(*ast.SelectStmt)
				condition, err := ParseCondition(sel.Where, c.args...)
				assert.Nil(t, err)
				cond := condition.(ConditionShard)
				slice, err := cond.Shard(shardAlg)
				assert.Nil(t, err)
				fmt.Println(slice)
			}
		})
	}
}

func TestNumberRangeShard(t *testing.T) {
	var shardAlg = mockNumberRangeAlgorithm(t)

	testCases := []*struct {
		sql  string
		args []interface{}
	}{
		{
			sql:  "select * from student where 1 != 1",
			args: []interface{}{5, 18},
		},
		{
			sql:  "select * from student where ? = uid or uid = ?",
			args: []interface{}{5, 100008},
		},
		{
			sql:  "select * from student where uid = ? or age = ?",
			args: []interface{}{200008, 18},
		},
		{
			// uid = 5 or ((age = 18 and name = jane) or (age = 18 and gender = 1))
			sql:  "select * from student where uid = ? or (age = ? and (name = ? or gender = ?))",
			args: []interface{}{30007, 18, "jane", 1},
		},
		{
			sql:  "select * from student where uid = ? and (age = ? or name = ?)",
			args: []interface{}{5, 18, "jane"},
		},
		{
			sql:  "select * from student where uid > ? and uid < ? and age = ?",
			args: []interface{}{109997, 200007, 18},
		},
		{
			sql:  "select * from student where uid > ? and uid < ? and age = ?",
			args: []interface{}{7880, 7890, 18},
		},
	}

	p := parser.New()
	for _, c := range testCases {
		t.Run(c.sql, func(t *testing.T) {
			stmt, err := p.ParseOneStmt(c.sql, "", "")
			if assert.NoErrorf(t, err, "parse sql statement err: %s", err) {
				stmt.Accept(&visitor.ParamVisitor{})
				sel := stmt.(*ast.SelectStmt)
				condition, err := ParseCondition(sel.Where, c.args...)
				assert.Nil(t, err)
				cond := condition.(ConditionShard)
				slice, err := cond.Shard(shardAlg)
				assert.Nil(t, err)
				fmt.Println(slice)
			}
		})
	}
}

func mockTopology() *topo.Topology {
	tp, _ := topo.ParseTopology("school", "student", map[int]string{
		0: "0-9",
		1: "10-19",
		2: "20-29",
		3: "30-39",
		4: "40-49",
		5: "50-59",
		6: "60-69",
		7: "70-79",
		8: "80-89",
		9: "90-99",
	})
	return tp
}

func mockNumberRangeAlgorithm(t *testing.T) *NumberRange {
	rangeConfig := map[string]interface{}{
		"0": "0-100k",
		"1": "100k-200k",
		"2": "200k-300k",
		"3": "300k-400k",
		"4": "400k-500k",
		"5": "500k-600k",
		"6": "600k-700k",
		"7": "700k-800k",
		"8": "800k-900k",
		"9": "900k-1000k",
	}

	ranges, err := parseNumberRangeConfig(rangeConfig)
	assert.Nil(t, err)
	return &NumberRange{
		shardingKey:   "uid",
		allowFullScan: false,
		topology:      mockTopology(),
		ranges:        ranges,
	}
}
