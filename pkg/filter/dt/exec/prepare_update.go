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
	"github.com/cectc/dbpack/third_party/parser/ast"
	"github.com/cectc/dbpack/third_party/parser/format"
)

type prepareUpdateExecutor struct {
	conn        *driver.BackendConnection
	stmt        *ast.UpdateStmt
	args        map[string]interface{}
	beforeImage *schema.TableRecords
}

func NewPrepareUpdateExecutor(
	conn *driver.BackendConnection,
	stmt *ast.UpdateStmt,
	args map[string]interface{},
	beforeImage *schema.TableRecords) Executor {
	return &prepareUpdateExecutor{
		conn:        conn,
		stmt:        stmt,
		args:        args,
		beforeImage: beforeImage,
	}
}

func (executor *prepareUpdateExecutor) BeforeImage(ctx context.Context) (*schema.TableRecords, error) {
	tableMeta, err := executor.GetTableMeta(ctx)
	if err != nil {
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

	result, _, err := executor.conn.PrepareQueryArgs(sql, args)
	if err != nil {
		return nil, err
	}
	return schema.BuildBinaryRecords(tableMeta, result), nil
}

func (executor *prepareUpdateExecutor) AfterImage(ctx context.Context) (*schema.TableRecords, error) {
	if executor.beforeImage == nil || len(executor.beforeImage.Rows) == 0 {
		return nil, nil
	}

	tableMeta, err := executor.GetTableMeta(ctx)
	if err != nil {
		return nil, err
	}

	afterImageSql := executor.buildAfterImageSql(tableMeta, executor.beforeImage)
	var args = make([]interface{}, 0)
	for _, field := range executor.beforeImage.PKFields() {
		args = append(args, field.Value)
	}
	result, _, err := executor.conn.PrepareQueryArgs(afterImageSql, args)
	if err != nil {
		return nil, err
	}
	return schema.BuildBinaryRecords(tableMeta, result), nil
}

func (executor *prepareUpdateExecutor) GetTableMeta(ctx context.Context) (schema.TableMeta, error) {
	dbName := executor.conn.DataSourceName()
	db := resource.GetDBManager().GetDB(dbName)
	return meta.GetTableMetaCache().GetTableMeta(ctx, db, executor.GetTableName())
}

func (executor *prepareUpdateExecutor) GetTableName() string {
	var sb strings.Builder
	if err := executor.stmt.TableRefs.TableRefs.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
		log.Panic(err)
	}
	return sb.String()
}

func (executor *prepareUpdateExecutor) buildBeforeImageSql(tableMeta schema.TableMeta) string {
	var b strings.Builder
	b.WriteString("SELECT ")
	var i = 0
	columnCount := len(tableMeta.Columns)
	for _, column := range tableMeta.Columns {
		b.WriteString(misc.CheckAndReplace(column))
		i = i + 1
		if i != columnCount {
			b.WriteByte(',')
		} else {
			b.WriteByte(' ')
		}
	}
	b.WriteString(fmt.Sprintf(" FROM %s WHERE ", executor.GetTableName()))
	b.WriteString(executor.GetWhereCondition())
	b.WriteString(" FOR UPDATE")
	return b.String()
}

func (executor *prepareUpdateExecutor) buildAfterImageSql(tableMeta schema.TableMeta, beforeImage *schema.TableRecords) string {
	var b strings.Builder
	b.WriteString("SELECT ")
	var i = 0
	columnCount := len(tableMeta.Columns)
	for _, columnName := range tableMeta.Columns {
		b.WriteString(misc.CheckAndReplace(columnName))
		i = i + 1
		if i != columnCount {
			b.WriteByte(',')
		} else {
			b.WriteByte(' ')
		}
	}
	b.WriteString(fmt.Sprintf(" FROM %s ", executor.GetTableName()))
	b.WriteString(fmt.Sprintf("WHERE `%s` IN", tableMeta.GetPKName()))
	b.WriteString(misc.MysqlAppendInParam(len(beforeImage.PKFields())))
	return b.String()
}

func (executor *prepareUpdateExecutor) GetWhereCondition() string {
	var sb strings.Builder
	if err := executor.stmt.Where.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
		log.Error(err)
	}
	return sb.String()
}

func (executor *prepareUpdateExecutor) GetUpdateColumns() []string {
	columns := make([]string, 0)

	for _, assignment := range executor.stmt.List {
		columns = append(columns, assignment.Column.Name.String())
	}
	return columns
}
