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
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.com/cectc/dbpack/pkg/constant"
	"github.com/cectc/dbpack/pkg/driver"
	"github.com/cectc/dbpack/pkg/dt"
	"github.com/cectc/dbpack/pkg/dt/schema"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/pkg/visitor"
	"github.com/cectc/dbpack/third_party/parser"
	"github.com/cectc/dbpack/third_party/parser/ast"
)

var (
	count     = 0
	err       = errors.New("resource locked!")
	tableMeta = schema.TableMeta{
		SchemaName: "db",
		TableName:  "t",
		Columns:    []string{"id", "age"},
		AllColumns: map[string]schema.ColumnMeta{
			"id": {
				TableCat:        "def",
				TableSchemeName: "db",
				TableName:       "t",
				ColumnName:      "id",
				DataType:        -5,
				DataTypeName:    "bigint",
				ColumnSize:      0,
				DecimalDigits:   19,
				NumPrecRadix:    0,
				Nullable:        0,
				Remarks:         "",
				ColumnDef:       "",
				SqlDataType:     0,
				SqlDatetimeSub:  0,
				CharOctetLength: 0,
				OrdinalPosition: 1,
				IsNullable:      "NO",
				IsAutoIncrement: "auto_increment",
			},
			"age": {
				TableCat:        "def",
				TableSchemeName: "table",
				TableName:       "t",
				ColumnName:      "age",
				DataType:        0,
				DataTypeName:    "int",
				ColumnSize:      0,
				DecimalDigits:   10,
				NumPrecRadix:    0,
				Nullable:        0,
				Remarks:         "",
				ColumnDef:       "",
				SqlDataType:     0,
				SqlDatetimeSub:  0,
				CharOctetLength: 0,
				OrdinalPosition: 2,
				IsNullable:      "NO",
				IsAutoIncrement: "",
			},
		},
		AllIndexes: map[string]schema.IndexMeta{
			"id": {
				Values: []schema.ColumnMeta{
					{
						TableCat:        "def",
						TableSchemeName: "db",
						TableName:       "t",
						ColumnName:      "id",
						DataType:        -5,
						DataTypeName:    "bigint",
						ColumnSize:      0,
						DecimalDigits:   19,
						NumPrecRadix:    0,
						Nullable:        0,
						Remarks:         "",
						ColumnDef:       "",
						SqlDataType:     0,
						SqlDatetimeSub:  0,
						CharOctetLength: 0,
						OrdinalPosition: 1,
						IsNullable:      "NO",
						IsAutoIncrement: "auto_increment",
					},
				},
				NonUnique:       false,
				IndexQualifier:  "",
				IndexName:       "PRIMARY",
				ColumnName:      "id",
				Type:            0,
				IndexType:       schema.IndexTypePrimary,
				AscOrDesc:       "A",
				Cardinality:     1,
				OrdinalPosition: 1,
			},
		},
	}
)

func TestGlobalLock(t *testing.T) {
	testCases := []*struct {
		sql                    string
		isUpdate               bool
		lockInterval           time.Duration
		lockTimes              int
		expectedTableName      string
		expectedWhereCondition string
		expectedBeforeImageSql string
		expectedErr            error
	}{
		{
			sql:                    "delete /*+ GlobalLock() */ from T where id = ?",
			isUpdate:               false,
			lockInterval:           5 * time.Millisecond,
			lockTimes:              3,
			expectedTableName:      "`T`",
			expectedWhereCondition: "`id`=?",
			expectedBeforeImageSql: "SELECT id,age  FROM `T` WHERE `id`=?",
			expectedErr:            err,
		},
		{
			sql:                    "delete /*+ GlobalLock() */ from T where id = ?",
			isUpdate:               false,
			lockInterval:           5 * time.Millisecond,
			lockTimes:              10,
			expectedTableName:      "`T`",
			expectedWhereCondition: "`id`=?",
			expectedBeforeImageSql: "SELECT id,age  FROM `T` WHERE `id`=?",
			expectedErr:            nil,
		},
		{
			sql:                    "update /*+ GlobalLock() */ T set age = 18 where id = ?",
			isUpdate:               true,
			lockInterval:           5 * time.Millisecond,
			lockTimes:              3,
			expectedTableName:      "`T`",
			expectedWhereCondition: "`id`=?",
			expectedBeforeImageSql: "SELECT id,age  FROM `T` WHERE `id`=?",
			expectedErr:            err,
		},
		{
			sql:                    "update /*+ GlobalLock() */ T set age = 18 where id = ?",
			isUpdate:               true,
			lockInterval:           5 * time.Millisecond,
			lockTimes:              10,
			expectedTableName:      "`T`",
			expectedWhereCondition: "`id`=?",
			expectedBeforeImageSql: "SELECT id,age  FROM `T` WHERE `id`=?",
			expectedErr:            nil,
		},
	}

	patches1 := isLockablePatch()
	defer patches1.Reset()

	patches2 := beforeImagePatch()
	defer patches2.Reset()

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
				PrepareStmt: c.sql,
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

			var executor Executable
			if c.isUpdate {
				updateStmt := stmt.(*ast.UpdateStmt)
				executor = NewPrepareGlobalLockExecutor(&driver.BackendConnection{}, c.isUpdate, nil, updateStmt, protoStmt.BindVars)
			} else {
				deleteStmt := stmt.(*ast.DeleteStmt)
				executor = NewPrepareGlobalLockExecutor(&driver.BackendConnection{}, c.isUpdate, deleteStmt, nil, protoStmt.BindVars)
			}
			tableName := executor.GetTableName()
			assert.Equal(t, c.expectedTableName, tableName)
			whereCondition := executor.(*prepareGlobalLockExecutor).GetWhereCondition()
			assert.Equal(t, c.expectedWhereCondition, whereCondition)
			beforeImageSql := executor.(*prepareGlobalLockExecutor).buildBeforeImageSql(tableMeta)
			assert.Equal(t, c.expectedBeforeImageSql, beforeImageSql)
			_, executeErr := executor.Executable(ctx, c.lockInterval, c.lockTimes)
			assert.Equal(t, c.expectedErr, executeErr)
		})
	}
}

func isLockablePatch() *gomonkey.Patches {
	var transactionManager *dt.DistributedTransactionManager
	return gomonkey.ApplyMethodFunc(transactionManager, "IsLockable", func(ctx context.Context, resourceID, lockKey string) (bool, error) {
		count++
		if count < 5 {
			return false, err
		}
		return true, nil
	})
}

func beforeImagePatch() *gomonkey.Patches {
	var executor *prepareGlobalLockExecutor
	return gomonkey.ApplyMethodFunc(executor, "BeforeImage", func(ctx context.Context) (*schema.TableRecords, error) {
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
							Name:    "age",
							KeyType: schema.Null,
							Type:    0,
							Value:   "20",
						},
					},
				},
			},
		}, nil
	})
}
