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

package exec

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cectc/dbpack/pkg/driver"
	"github.com/cectc/dbpack/pkg/visitor"
	"github.com/cectc/dbpack/third_party/parser"
	"github.com/cectc/dbpack/third_party/parser/ast"
)

func TestPrepareInsert(t *testing.T) {
	testCases := []*struct {
		sql                   string
		expectedTableName     string
		expectedInsertColumns []string
		expectedAfterImageSql string
	}{
		{
			sql:                   "insert /*+ XID('gs/svc/3531093008562585601') */ into T(id, name, age) values(?, ?, ?)",
			expectedTableName:     "`T`",
			expectedInsertColumns: []string{"id", "name", "age"},
			expectedAfterImageSql: "SELECT id,name,age FROM `T` WHERE `id` IN (?)",
		},
	}

	for _, c := range testCases {
		t.Run(c.sql, func(t *testing.T) {
			p := parser.New()
			stmt, err := p.ParseOneStmt(c.sql, "", "")
			if err != nil {
				t.Error(err)
				return
			}
			stmt.Accept(&visitor.ParamVisitor{})

			insert := stmt.(*ast.InsertStmt)
			executor := NewPrepareInsertExecutor(&driver.BackendConnection{}, insert, map[string]interface{}{
				"v1": 10,
				"v2": "scott",
				"v3": 18,
			}, nil)

			tableName := executor.GetTableName()
			assert.Equal(t, c.expectedTableName, tableName)
			insertColumns := executor.(*prepareInsertExecutor).GetInsertColumns()
			assert.Equal(t, c.expectedInsertColumns, insertColumns)
			afterImageSql := executor.(*prepareInsertExecutor).buildAfterImageSql(tableMeta, []interface{}{10})
			assert.Equal(t, c.expectedAfterImageSql, afterImageSql)
		})
	}
}
