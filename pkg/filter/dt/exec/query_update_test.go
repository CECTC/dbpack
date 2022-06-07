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
	"github.com/cectc/dbpack/pkg/dt/schema"
	"github.com/cectc/dbpack/pkg/visitor"
	"github.com/cectc/dbpack/third_party/parser"
	"github.com/cectc/dbpack/third_party/parser/ast"
)

func TestQueryUpdate(t *testing.T) {
	testCases := []*struct {
		sql                    string
		expectedTableName      string
		expectedUpdateColumns  []string
		expectedBeforeImageSql string
		expectedAfterImageSql  string
	}{
		{
			sql:                    "update /*+ XID('gs/svc/3531093008562585601') */ T set name = ?, age = ? where id = ?",
			expectedTableName:      "`T`",
			expectedUpdateColumns:  []string{"name", "age"},
			expectedBeforeImageSql: "SELECT id,name,age FROM `T` WHERE `id`=? FOR UPDATE",
			expectedAfterImageSql:  "SELECT id,name,age FROM `T` WHERE `id` IN ('10')",
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

			udpateStmt := stmt.(*ast.UpdateStmt)
			executor := NewQueryUpdateExecutor(&driver.BackendConnection{}, udpateStmt, &schema.TableRecords{
				TableMeta: tableMeta,
				TableName: "T",
				Rows: []*schema.Row{
					{
						Fields: []*schema.Field{
							{
								Name:    "id",
								KeyType: schema.PrimaryKey,
								Type:    0,
								Value:   "10",
							},
							{
								Name:    "name",
								KeyType: schema.Null,
								Type:    0,
								Value:   []byte("scott"),
							},
							{
								Name:    "age",
								KeyType: schema.Null,
								Type:    0,
								Value:   "20",
							},
						},
					},
				},
			})

			tableName := executor.GetTableName()
			assert.Equal(t, c.expectedTableName, tableName)
			updateColumns := executor.(*queryUpdateExecutor).GetUpdateColumns()
			assert.Equal(t, c.expectedUpdateColumns, updateColumns)
			beforeImageSql := executor.(*queryUpdateExecutor).buildBeforeImageSql(tableMeta)
			assert.Equal(t, c.expectedBeforeImageSql, beforeImageSql)
			afterImageSql := executor.(*queryUpdateExecutor).buildAfterImageSql(tableMeta)
			assert.Equal(t, c.expectedAfterImageSql, afterImageSql)
		})
	}
}
