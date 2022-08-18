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
	"strconv"
	"strings"

	"github.com/cectc/dbpack/pkg/constant"
	"github.com/cectc/dbpack/pkg/driver"
	"github.com/cectc/dbpack/pkg/dt/schema"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/meta"
	"github.com/cectc/dbpack/pkg/misc"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/pkg/resource"
	"github.com/cectc/dbpack/pkg/tracing"
	"github.com/cectc/dbpack/third_party/parser/ast"
	"github.com/cectc/dbpack/third_party/parser/format"
)

type prepareInsertExecutor struct {
	appid  string
	conn   *driver.BackendConnection
	stmt   *ast.InsertStmt
	args   map[string]interface{}
	result proto.Result
}

func NewPrepareInsertExecutor(
	appid string,
	conn *driver.BackendConnection,
	stmt *ast.InsertStmt,
	args map[string]interface{},
	result proto.Result) Executor {
	return &prepareInsertExecutor{
		appid:  appid,
		conn:   conn,
		stmt:   stmt,
		args:   args,
		result: result,
	}
}

func (executor *prepareInsertExecutor) BeforeImage(ctx context.Context) (*schema.TableRecords, error) {
	return nil, nil
}

func (executor *prepareInsertExecutor) AfterImage(ctx context.Context) (*schema.TableRecords, error) {
	spanCtx, span := tracing.GetTraceSpan(ctx, tracing.ExecutorFetchAfterImage)
	defer span.End()
	var afterImage *schema.TableRecords
	var err error
	pkValues, err := executor.getPKValuesByColumn(ctx)
	if err != nil {
		tracing.RecordErrorSpan(span, err)
		return nil, err
	}
	if executor.getPKIndex(ctx) >= 0 {
		afterImage, err = executor.buildTableRecords(spanCtx, pkValues)
	} else {
		pk, _ := executor.result.LastInsertId()
		afterImage, err = executor.buildTableRecords(spanCtx, []interface{}{pk})
	}
	if err != nil {
		tracing.RecordErrorSpan(span, err)
		return nil, err
	}
	return afterImage, nil
}

func (executor *prepareInsertExecutor) GetTableMeta(ctx context.Context) (schema.TableMeta, error) {
	dbName := executor.conn.DataSourceName()
	db := resource.GetDBManager(executor.appid).GetDB(dbName)
	return meta.GetTableMetaCache().GetTableMeta(ctx, db, executor.GetTableName())
}

func (executor *prepareInsertExecutor) GetTableName() string {
	var sb strings.Builder
	if err := executor.stmt.Table.TableRefs.Left.Restore(format.NewRestoreCtx(constant.DBPackRestoreFormat, &sb)); err != nil {
		log.Panic(err)
	}
	return sb.String()
}

func (executor *prepareInsertExecutor) buildTableRecords(ctx context.Context, pkValues []interface{}) (*schema.TableRecords, error) {
	tableMeta, err := executor.GetTableMeta(ctx)
	if err != nil {
		return nil, err
	}

	afterImageSql := executor.buildAfterImageSql(tableMeta, pkValues)
	result, _, err := executor.conn.PrepareQueryArgs(ctx, afterImageSql, pkValues)
	if err != nil {
		return nil, err
	}
	return schema.BuildTableRecords(tableMeta, result), nil
}

func (executor *prepareInsertExecutor) buildAfterImageSql(tableMeta schema.TableMeta, pkValues []interface{}) string {
	var b strings.Builder
	b.WriteString("SELECT ")
	columnCount := len(tableMeta.Columns)
	for i, column := range tableMeta.Columns {
		b.WriteString(misc.CheckAndReplace(column))
		if i < columnCount-1 {
			b.WriteByte(',')
		} else {
			b.WriteByte(' ')
		}
	}
	b.WriteString(fmt.Sprintf("FROM %s ", executor.GetTableName()))
	b.WriteString(fmt.Sprintf("WHERE `%s` IN ", tableMeta.GetPKName()))
	b.WriteString(misc.MysqlAppendInParam(len(pkValues)))
	return b.String()
}

func (executor *prepareInsertExecutor) getPKValuesByColumn(ctx context.Context) ([]interface{}, error) {
	pkValues := make([]interface{}, 0)
	columnLen := executor.getColumnLen(ctx)
	pkIndex := executor.getPKIndex(ctx)
	for key, value := range executor.args {
		i, err := strconv.Atoi(key[1:])
		if err != nil {
			return nil, err
		}
		if i%columnLen == pkIndex+1 {
			pkValues = append(pkValues, value)
		}
	}
	return pkValues, nil
}

func (executor *prepareInsertExecutor) getPKIndex(ctx context.Context) int {
	insertColumns := executor.GetInsertColumns()
	tableMeta, _ := executor.GetTableMeta(ctx)

	for i, columnName := range insertColumns {
		if strings.EqualFold(tableMeta.GetPKName(), columnName) {
			return i
		}
	}

	allColumns := tableMeta.Columns
	for i, column := range allColumns {
		if strings.EqualFold(tableMeta.GetPKName(), column) {
			return i
		}
	}
	return -1
}

func (executor *prepareInsertExecutor) getColumnLen(ctx context.Context) int {
	insertColumns := executor.GetInsertColumns()
	if insertColumns != nil {
		return len(insertColumns)
	}
	tableMeta, _ := executor.GetTableMeta(ctx)

	return len(tableMeta.Columns)
}

func (executor *prepareInsertExecutor) GetInsertColumns() []string {
	result := make([]string, 0)
	for _, col := range executor.stmt.Columns {
		result = append(result, col.Name.String())
	}
	return result
}
