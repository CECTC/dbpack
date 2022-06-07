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
	"fmt"
	"strings"

	"github.com/cectc/dbpack/pkg/driver"
	"github.com/cectc/dbpack/pkg/dt/schema"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/meta"
	"github.com/cectc/dbpack/pkg/misc"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/pkg/resource"
	"github.com/cectc/dbpack/third_party/parser/ast"
	"github.com/cectc/dbpack/third_party/parser/format"
)

type queryInsertExecutor struct {
	conn   *driver.BackendConnection
	stmt   *ast.InsertStmt
	result proto.Result
}

func NewQueryInsertExecutor(
	conn *driver.BackendConnection,
	stmt *ast.InsertStmt,
	result proto.Result) Executor {
	return &queryInsertExecutor{
		conn:   conn,
		stmt:   stmt,
		result: result,
	}
}

func (executor *queryInsertExecutor) BeforeImage(ctx context.Context) (*schema.TableRecords, error) {
	return nil, nil
}

func (executor *queryInsertExecutor) AfterImage(ctx context.Context) (*schema.TableRecords, error) {
	var afterImage *schema.TableRecords
	var err error
	pkValues, err := executor.getPKValuesByColumn(ctx)
	if err != nil {
		return nil, err
	}
	if executor.getPKIndex(ctx) >= 0 {
		afterImage, err = executor.buildTableRecords(ctx, pkValues)
	} else {
		pk, _ := executor.result.LastInsertId()
		afterImage, err = executor.buildTableRecords(ctx, []interface{}{pk})
	}
	if err != nil {
		return nil, err
	}
	return afterImage, nil
}

func (executor *queryInsertExecutor) buildTableRecords(ctx context.Context, pkValues []interface{}) (*schema.TableRecords, error) {
	tableMeta, err := executor.GetTableMeta(ctx)
	if err != nil {
		return nil, err
	}

	afterImageSql := executor.buildAfterImageSql(tableMeta, pkValues)
	result, _, err := executor.conn.ExecuteWithWarningCount(afterImageSql, true)
	if err != nil {
		return nil, err
	}
	return schema.BuildTextRecords(tableMeta, result), nil
}

func (executor *queryInsertExecutor) buildAfterImageSql(tableMeta schema.TableMeta, pkValues []interface{}) string {
	var b strings.Builder
	b.WriteString("SELECT ")
	var i = 0
	columnCount := len(tableMeta.Columns)
	for _, column := range tableMeta.Columns {
		b.WriteString(misc.CheckAndReplace(column))
		i = i + 1
		if i < columnCount {
			b.WriteByte(',')
		} else {
			b.WriteByte(' ')
		}
	}
	b.WriteString(fmt.Sprintf("FROM %s ", executor.GetTableName()))
	b.WriteString(fmt.Sprintf(" WHERE `%s` IN ", tableMeta.GetPKName()))
	b.WriteString(misc.MysqlAppendInParamWithValue(pkValues))
	return b.String()
}

func (executor *queryInsertExecutor) GetTableMeta(ctx context.Context) (schema.TableMeta, error) {
	dbName := executor.conn.DataSourceName()
	db := resource.GetDBManager().GetDB(dbName)
	return meta.GetTableMetaCache().GetTableMeta(ctx, db, executor.GetTableName())
}

func (executor *queryInsertExecutor) GetTableName() string {
	var sb strings.Builder
	if err := executor.stmt.Table.TableRefs.Left.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
		log.Panic(err)
	}
	return sb.String()
}

func (executor *queryInsertExecutor) getPKValuesByColumn(ctx context.Context) ([]interface{}, error) {
	pkValues := make([]interface{}, 0)
	pkIndex := executor.getPKIndex(ctx)
	for j := range executor.stmt.Lists {
		for i, value := range executor.stmt.Lists[j] {
			if i == pkIndex {
				var sb strings.Builder
				if err := value.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
					log.Panic(err)
				}
				pkValues = append(pkValues, sb.String())
				break
			}
		}
	}

	return pkValues, nil
}

func (executor *queryInsertExecutor) getPKIndex(ctx context.Context) int {
	insertColumns := executor.GetInsertColumns()
	tableMeta, _ := executor.GetTableMeta(ctx)

	if insertColumns != nil {
		for i, columnName := range insertColumns {
			if strings.EqualFold(tableMeta.GetPKName(), columnName) {
				return i
			}
		}
	} else {
		allColumns := tableMeta.Columns
		var idx = 0
		for i, column := range allColumns {
			if strings.EqualFold(tableMeta.GetPKName(), column) {
				return i
			}
		}
	}
	return -1
}

func (executor *queryInsertExecutor) getColumnLen(ctx context.Context) int {
	insertColumns := executor.GetInsertColumns()
	if insertColumns != nil {
		return len(insertColumns)
	}
	tableMeta, _ := executor.GetTableMeta(ctx)

	return len(tableMeta.Columns)
}

func (executor *queryInsertExecutor) GetInsertColumns() []string {
	result := make([]string, 0)
	for _, col := range executor.stmt.Columns {
		result = append(result, col.Name.String())
	}
	return result
}
