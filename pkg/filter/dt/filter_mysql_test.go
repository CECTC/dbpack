package dt

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
)

func TestGlobalLock(t *testing.T) {
	var (
		err    = errors.New("resource locked!")
		filter = &_mysqlFilter{
			applicationID: "testService",
		}
	)
	testCases := []*struct {
		sql          string
		lockInterval time.Duration
		lockTimes    int
		expectedErr  error
	}{
		{
			sql:          "delete /*+ GlobalLock() */ from T where id = ?",
			lockInterval: 5 * time.Millisecond,
			lockTimes:    3,
			expectedErr:  err,
		},
		{
			sql:          "delete /*+ GlobalLock() */ from T where id = ?",
			lockInterval: 5 * time.Millisecond,
			lockTimes:    10,
			expectedErr:  nil,
		},
		{
			sql:          "update /*+ GlobalLock() */ T set age = 18 and id = ?",
			lockInterval: 5 * time.Millisecond,
			lockTimes:    3,
			expectedErr:  err,
		},
		{
			sql:          "update /*+ GlobalLock() */ T set age = 18 and id = ?",
			lockInterval: 5 * time.Millisecond,
			lockTimes:    10,
			expectedErr:  nil,
		},
	}

	i := 0
	var transactionManager *dt.DistributedTransactionManager
	patches1 := gomonkey.ApplyMethodFunc(transactionManager, "IsLockable", func(ctx context.Context, resourceID, lockKey string) (bool, error) {
		i++
		if i < 5 {
			return false, err
		}
		return true, nil
	})
	defer patches1.Reset()

	var executor *globalLockExecutor
	patches2 := gomonkey.ApplyMethodFunc(executor, "BeforeImage", func(ctx context.Context) (*schema.TableRecords, error) {
		return &schema.TableRecords{
			TableMeta: schema.TableMeta{
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
			},
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
	defer patches2.Reset()

	for _, c := range testCases {
		t.Run(c.sql, func(t *testing.T) {
			i = 0
			p := parser.New()
			stmt, err := p.ParseOneStmt(c.sql, "", "")
			if err != nil {
				t.Error(err)
				return
			}
			stmt.Accept(&visitor.ParamVisitor{})

			ctx := proto.WithCommandType(context.Background(), constant.ComStmtExecute)
			ctx = proto.WithPrepareStmt(ctx, &proto.Stmt{
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
			})

			filter.lockRetryInterval = c.lockInterval
			filter.lockRetryTimes = c.lockTimes
			handleErr := filter.PreHandle(ctx, &driver.BackendConnection{})
			assert.Equal(t, c.expectedErr, handleErr)
		})
	}
}
