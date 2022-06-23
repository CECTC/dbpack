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
	"context"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"

	"github.com/cectc/dbpack/pkg/constant"
	"github.com/cectc/dbpack/pkg/driver"
	"github.com/cectc/dbpack/pkg/dt/schema"
	"github.com/cectc/dbpack/pkg/mysql"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/pkg/visitor"
	"github.com/cectc/dbpack/third_party/parser"
	"github.com/cectc/dbpack/third_party/parser/ast"
)

func TestQuerySelectForUpdate(t *testing.T) {
	testCases := []*struct {
		sql                    string
		lockInterval           time.Duration
		lockTimes              int
		expectedTableName      string
		expectedWhereCondition string
		expectedErr            error
	}{
		{
			sql:                    "select /*+ GlobalLock() */ * from T where id = 10 for update",
			lockInterval:           5 * time.Millisecond,
			lockTimes:              3,
			expectedTableName:      "`T`",
			expectedWhereCondition: "`id`=10",
			expectedErr:            err,
		},
	}

	patches1 := isLockablePatch()
	defer patches1.Reset()

	patches2 := getQueryTableMetaPatch()
	defer patches2.Reset()

	patches3 := buildTextRecordsPatch()
	defer patches3.Reset()

	for _, c := range testCases {
		t.Run(c.sql, func(t *testing.T) {
			count = 0
			p := parser.New()
			stmt, err := p.ParseOneStmt(c.sql, "", "")
			if err != nil {
				t.Error(err)
				return
			}
			stmt.Accept(&visitor.ParamVisitor{})

			ctx := proto.WithCommandType(context.Background(), constant.ComStmtExecute)
			protoStmt := &proto.Stmt{
				StatementID: 1,
				SqlText:     c.sql,
				ParamsCount: 1,
				ParamData:   nil,
				ParamsType:  nil,
				ColumnNames: nil,
				BindVars: map[string]interface{}{
					"v1": 10,
				},
				StmtNode: stmt,
			}
			ctx = proto.WithPrepareStmt(ctx, protoStmt)

			selectForUpdateStmt := stmt.(*ast.SelectStmt)
			executor := NewQuerySelectForUpdateExecutor(&driver.BackendConnection{}, selectForUpdateStmt, &mysql.Result{})
			tableName := executor.GetTableName()
			assert.Equal(t, c.expectedTableName, tableName)
			_, executeErr := executor.Executable(ctx, c.lockInterval, c.lockTimes)
			assert.Equal(t, c.expectedErr, executeErr)
		})
	}
}

func getQueryTableMetaPatch() *gomonkey.Patches {
	var executor *querySelectForUpdateExecutor
	return gomonkey.ApplyMethodFunc(executor, "GetTableMeta", func(ctx context.Context) (schema.TableMeta, error) {
		return tableMeta, nil
	})
}

func buildTextRecordsPatch() *gomonkey.Patches {
	return gomonkey.ApplyFunc(schema.BuildTextRecords, func(meta schema.TableMeta, result *mysql.Result) *schema.TableRecords {
		return &schema.TableRecords{
			TableMeta: tableMeta,
			TableName: "t",
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
							Value:   "scott",
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
		}
	})
}
