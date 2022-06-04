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

func TestDeleteOnSingleDBPlan(t *testing.T) {
	testCases := []struct {
		deleteSql            string
		tables               []string
		expectedGenerateSqls []string
	}{
		{
			deleteSql: "delete from student where id in (?,?)",
			tables:    []string{"student_1", "student_5"},
			expectedGenerateSqls: []string{
				"DELETE FROM student_1 WHERE `id` IN (?,?)",
				"DELETE FROM student_5 WHERE `id` IN (?,?)",
			},
		},
		{
			deleteSql: "delete from student where id = 9",
			tables:    []string{"student_9"},
			expectedGenerateSqls: []string{
				"DELETE FROM student_9 WHERE `id`=9",
			},
		},
	}
	for _, c := range testCases {
		t.Run(c.deleteSql, func(t *testing.T) {
			p := parser.New()
			stmt, err := p.ParseOneStmt(c.deleteSql, "", "")
			if err != nil {
				t.Error(err)
				return
			}
			stmt.Accept(&visitor.ParamVisitor{})
			deleteStmt := stmt.(*ast.DeleteStmt)
			plan := &DeleteOnSingleDBPlan{
				Database: "school_0",
				Tables:   c.tables,
				Stmt:     deleteStmt,
				Args:     nil,
				Executor: nil,
			}
			for i, table := range plan.Tables {
				var sb strings.Builder
				err := plan.generate(&sb, table)
				assert.Nil(t, err)
				assert.Equal(t, c.expectedGenerateSqls[i], sb.String())
			}
		})
	}
}
