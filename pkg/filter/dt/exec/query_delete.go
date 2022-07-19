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

type queryDeleteExecutor struct {
	conn *driver.BackendConnection
	stmt *ast.DeleteStmt
}

func NewQueryDeleteExecutor(
	conn *driver.BackendConnection,
	stmt *ast.DeleteStmt) Executor {
	return &queryDeleteExecutor{
		conn: conn,
		stmt: stmt,
	}
}

func (executor *queryDeleteExecutor) BeforeImage(ctx context.Context) (*schema.TableRecords, error) {
	spanCtx, span := tracing.GetTraceSpan(ctx, tracing.ExecutorFetchBeforeImage)
	defer span.End()
	tableMeta, err := executor.GetTableMeta(spanCtx)
	if err != nil {
		tracing.RecordErrorSpan(span, err)
		return nil, err
	}
	sql := executor.buildBeforeImageSql(tableMeta)
	result, _, err := executor.conn.ExecuteWithWarningCount(spanCtx, sql, true)
	if err != nil {
		tracing.RecordErrorSpan(span, err)
		return nil, err
	}
	return schema.BuildTextRecords(tableMeta, result), nil
}

func (executor *queryDeleteExecutor) AfterImage(ctx context.Context) (*schema.TableRecords, error) {
	return nil, nil
}

func (executor *queryDeleteExecutor) GetTableMeta(ctx context.Context) (schema.TableMeta, error) {
	dbName := executor.conn.DataSourceName()
	db := resource.GetDBManager().GetDB(dbName)
	return meta.GetTableMetaCache().GetTableMeta(ctx, db, executor.GetTableName())
}

func (executor *queryDeleteExecutor) GetTableName() string {
	var sb strings.Builder
	if err := executor.stmt.TableRefs.TableRefs.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
		log.Panic(err)
	}
	return sb.String()
}

func (executor *queryDeleteExecutor) buildBeforeImageSql(tableMeta schema.TableMeta) string {
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

func (executor *queryDeleteExecutor) GetWhereCondition() string {
	var sb strings.Builder
	if err := executor.stmt.Where.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
		log.Panic(err)
	}
	return sb.String()
}
