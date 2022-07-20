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

type queryUpdateExecutor struct {
	conn        *driver.BackendConnection
	stmt        *ast.UpdateStmt
	beforeImage *schema.TableRecords
}

func NewQueryUpdateExecutor(
	conn *driver.BackendConnection,
	stmt *ast.UpdateStmt,
	beforeImage *schema.TableRecords) Executor {
	return &queryUpdateExecutor{
		conn:        conn,
		stmt:        stmt,
		beforeImage: beforeImage,
	}
}

func (executor *queryUpdateExecutor) BeforeImage(ctx context.Context) (*schema.TableRecords, error) {
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

func (executor *queryUpdateExecutor) AfterImage(ctx context.Context) (*schema.TableRecords, error) {
	if executor.beforeImage == nil || len(executor.beforeImage.Rows) == 0 {
		return nil, nil
	}
	spanCtx, span := tracing.GetTraceSpan(ctx, tracing.ExecutorFetchAfterImage)
	defer span.End()
	tableMeta, err := executor.GetTableMeta(spanCtx)
	if err != nil {
		tracing.RecordErrorSpan(span, err)
		return nil, err
	}

	afterImageSql := executor.buildAfterImageSql(tableMeta)
	result, _, err := executor.conn.ExecuteWithWarningCount(spanCtx, afterImageSql, true)
	if err != nil {
		tracing.RecordErrorSpan(span, err)
		return nil, err
	}
	return schema.BuildTextRecords(tableMeta, result), nil
}

func (executor *queryUpdateExecutor) GetTableMeta(ctx context.Context) (schema.TableMeta, error) {
	dbName := executor.conn.DataSourceName()
	db := resource.GetDBManager().GetDB(dbName)
	return meta.GetTableMetaCache().GetTableMeta(ctx, db, executor.GetTableName())
}

func (executor *queryUpdateExecutor) GetTableName() string {
	var sb strings.Builder
	if err := executor.stmt.TableRefs.TableRefs.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
		log.Panic(err)
	}
	return sb.String()
}

func (executor *queryUpdateExecutor) buildBeforeImageSql(tableMeta schema.TableMeta) string {
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

func (executor *queryUpdateExecutor) buildAfterImageSql(tableMeta schema.TableMeta) string {
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
	b.WriteString(fmt.Sprintf("WHERE `%s` IN (", tableMeta.GetPKName()))
	pkFields := executor.beforeImage.PKFields()
	for i, field := range pkFields {
		switch val := field.Value.(type) {
		case string:
			b.WriteString(fmt.Sprintf("'%s'", val))
		case []byte:
			b.WriteString(fmt.Sprintf("'%s'", val))
		default:
			b.WriteString(fmt.Sprintf("%v", val))
		}
		if i < len(pkFields)-1 {
			b.WriteByte(',')
		}
	}
	b.WriteByte(')')
	return b.String()
}

func (executor *queryUpdateExecutor) GetWhereCondition() string {
	var sb strings.Builder
	if err := executor.stmt.Where.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
		log.Panic(err)
	}
	return sb.String()
}

func (executor *queryUpdateExecutor) GetUpdateColumns() []string {
	columns := make([]string, 0)

	for _, assignment := range executor.stmt.List {
		columns = append(columns, assignment.Column.Name.String())
	}
	return columns
}
