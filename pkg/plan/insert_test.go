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

package plan

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cectc/dbpack/pkg/visitor"
	"github.com/cectc/dbpack/third_party/parser"
	"github.com/cectc/dbpack/third_party/parser/ast"
)

func TestInsertPlan(t *testing.T) {
	testCases := []struct {
		insertSql           string
		table               string
		expectedGenerateSql string
	}{
		{
			insertSql:           "insert into student(id, name, gender, age) values(?,?,?,?)",
			table:               "student_5",
			expectedGenerateSql: "INSERT INTO student_5(id,name,gender,age) VALUES (?,?,?,?)",
		},
	}

	for _, c := range testCases {
		t.Run(c.insertSql, func(t *testing.T) {
			p := parser.New()
			stmt, err := p.ParseOneStmt(c.insertSql, "", "")
			if err != nil {
				t.Error(err)
				return
			}
			stmt.Accept(&visitor.ParamVisitor{})
			insertStmt := stmt.(*ast.InsertStmt)
			plan := &InsertPlan{
				Database: "school_0",
				Table:    c.table,
				Columns:  []string{"id", "name", "gender", "age"},
				Stmt:     insertStmt,
				Args:     nil,
				Executor: nil,
			}
			var sb strings.Builder
			err = plan.generate(&sb)
			assert.Nil(t, err)
			assert.Equal(t, c.expectedGenerateSql, sb.String())
		})
	}
}
