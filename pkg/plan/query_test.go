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
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cectc/dbpack/pkg/visitor"
	"github.com/cectc/dbpack/third_party/parser"
	"github.com/cectc/dbpack/third_party/parser/ast"
)

func TestQueryOnSingleDBPlan(t *testing.T) {
	testCases := []struct {
		selectSql           string
		tables              []string
		pk                  string
		args                []interface{}
		expectedGenerateSql string
	}{
		{
			selectSql:           "select * from student where id in (?,?)",
			tables:              []string{"student_1", "student_5"},
			pk:                  "id",
			args:                []interface{}{1, 5},
			expectedGenerateSql: "SELECT * FROM ((SELECT * FROM `student_1` WHERE `id` IN (?,?)) UNION ALL (SELECT * FROM `student_5` WHERE `id` IN (?,?))) t ORDER BY `id` ASC",
		},
		{
			selectSql:           "select * from student where id in (?,?) order by id desc",
			tables:              []string{"student_1", "student_5"},
			pk:                  "id",
			args:                []interface{}{1, 5},
			expectedGenerateSql: "SELECT * FROM ((SELECT * FROM `student_1` WHERE `id` IN (?,?) ORDER BY `id` DESC) UNION ALL (SELECT * FROM `student_5` WHERE `id` IN (?,?) ORDER BY `id` DESC)) t ORDER BY `id` DESC",
		},
		{
			selectSql:           "select * from student where id in (?,?) order by id desc limit ?, ?",
			tables:              []string{"student_1", "student_5"},
			pk:                  "id",
			args:                []interface{}{1, 5, 1000, 20},
			expectedGenerateSql: "SELECT * FROM ((SELECT * FROM `student_1` WHERE `id` IN (?,?) ORDER BY `id` DESC LIMIT 1020) UNION ALL (SELECT * FROM `student_5` WHERE `id` IN (?,?) ORDER BY `id` DESC LIMIT 1020)) t ORDER BY `id` DESC",
		},
	}

	for _, c := range testCases {
		t.Run(c.selectSql, func(t *testing.T) {
			p := parser.New()
			stmt, err := p.ParseOneStmt(c.selectSql, "", "")
			if err != nil {
				t.Error(err)
				return
			}
			stmt.Accept(&visitor.ParamVisitor{})
			selectStmt := stmt.(*ast.SelectStmt)
			plan := &QueryOnSingleDBPlan{
				Database: "school_0",
				Tables:   c.tables,
				PK:       c.pk,
				Stmt:     selectStmt,
				Args:     c.args,
				Executor: nil,
			}
			var (
				sb   strings.Builder
				args []interface{}
			)
			plan.castLimit()
			err = plan.generate(context.Background(), &sb, &args)
			assert.Nil(t, err)
			assert.Equal(t, c.expectedGenerateSql, sb.String())
		})
	}
}
