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

	"github.com/cectc/dbpack/pkg/visitor"
	"github.com/cectc/dbpack/third_party/parser"
	"github.com/cectc/dbpack/third_party/parser/ast"
)

func TestParseCondition(t *testing.T) {
	testCases := []*struct {
		sql  string
		args []interface{}
	}{
		{
			sql:  "select * from student where ? = uid or uid = ?",
			args: []interface{}{5, 8},
		},
		{
			sql:  "select * from student where uid = ? or uid = ?",
			args: []interface{}{5, 8},
		},
		{
			sql:  "select * from student where uid = ? and age = ?",
			args: []interface{}{5, 18},
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
				fmt.Println(condition)
			}
		})
	}
}
