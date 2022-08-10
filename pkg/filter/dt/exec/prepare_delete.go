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
	"github.com/cectc/dbpack/pkg/resource"
	"github.com/cectc/dbpack/pkg/tracing"
	"github.com/cectc/dbpack/third_party/parser/ast"
	"github.com/cectc/dbpack/third_party/parser/format"
)

type prepareDeleteExecutor struct {
	appid string
	conn  *driver.BackendConnection
	stmt  *ast.DeleteStmt
	args  map[string]interface{}
}

func NewPrepareDeleteExecutor(
	appid string,
	conn *driver.BackendConnection,
	stmt *ast.DeleteStmt,
	args map[string]interface{}) Executor {
	return &prepareDeleteExecutor{
		appid: appid,
		conn:  conn,
		stmt:  stmt,
		args:  args,
	}
}

func (executor *prepareDeleteExecutor) BeforeImage(ctx context.Context) (*schema.TableRecords, error) {
	spanCtx, span := tracing.GetTraceSpan(ctx, tracing.ExecutorFetchBeforeImage)
	defer span.End()
	tableMeta, err := executor.GetTableMeta(spanCtx)
	if err != nil {
		tracing.RecordErrorSpan(span, err)
		return nil, err
	}
	sql := executor.buildBeforeImageSql(tableMeta)
	var args []interface{}
	argsCount := strings.Count(sql, "?")
	begin := len(executor.args) - argsCount
	for ; begin < len(executor.args); begin++ {
		parameterID := fmt.Sprintf("v%d", begin+1)
		args = append(args, executor.args[parameterID])
	}

	result, _, err := executor.conn.PrepareQueryArgs(spanCtx, sql, args)
	if err != nil {
		tracing.RecordErrorSpan(span, err)
		return nil, err
	}
	return schema.BuildTableRecords(tableMeta, result), nil
}

func (executor *prepareDeleteExecutor) AfterImage(ctx context.Context) (*schema.TableRecords, error) {
	return nil, nil
}

func (executor *prepareDeleteExecutor) GetTableMeta(ctx context.Context) (schema.TableMeta, error) {
	dbName := executor.conn.DataSourceName()
	db := resource.GetDBManager(executor.appid).GetDB(dbName)
	return meta.GetTableMetaCache().GetTableMeta(ctx, db, executor.GetTableName())
}

func (executor *prepareDeleteExecutor) GetTableName() string {
	var sb strings.Builder
	if err := executor.stmt.TableRefs.TableRefs.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
		log.Panic(err)
	}
	return sb.String()
}

func (executor *prepareDeleteExecutor) buildBeforeImageSql(tableMeta schema.TableMeta) string {
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
	b.WriteString(fmt.Sprintf("FROM %s WHERE ", executor.GetTableName()))
	b.WriteString(executor.GetWhereCondition())
	b.WriteString(" FOR UPDATE")
	return b.String()
}

func (executor *prepareDeleteExecutor) GetWhereCondition() string {
	var sb strings.Builder
	if err := executor.stmt.Where.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
		log.Panic(err)
	}
	return sb.String()
}
