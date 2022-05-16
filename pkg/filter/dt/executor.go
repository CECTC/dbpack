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

package dt

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cectc/dbpack/pkg/constant"
	"github.com/cectc/dbpack/pkg/driver"
	"github.com/cectc/dbpack/pkg/dt"
	"github.com/cectc/dbpack/pkg/dt/schema"
	"github.com/cectc/dbpack/pkg/dt/undolog"
	"github.com/cectc/dbpack/pkg/meta"
	"github.com/cectc/dbpack/pkg/misc"
	"github.com/cectc/dbpack/pkg/mysql"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/pkg/resource"
	"github.com/cectc/dbpack/third_party/parser/ast"
	"github.com/cectc/dbpack/third_party/parser/format"
)

type insertExecutor struct {
	conn *driver.BackendConnection
	stmt *ast.InsertStmt
	args map[string]interface{}
}

type deleteExecutor struct {
	conn *driver.BackendConnection
	stmt *ast.DeleteStmt
	args map[string]interface{}
}

type selectForUpdateExecutor struct {
	conn              *driver.BackendConnection
	stmt              *ast.SelectStmt
	args              map[string]interface{}
	lockRetryInterval time.Duration
	lockRetryTimes    int
}

type updateExecutor struct {
	conn *driver.BackendConnection
	stmt *ast.UpdateStmt
	args map[string]interface{}
}

type globalLockExecutor struct {
	conn       *driver.BackendConnection
	isUpdate   bool
	deleteStmt *ast.DeleteStmt
	updateStmt *ast.UpdateStmt
	args       map[string]interface{}
}

func (executor *insertExecutor) GetTableName() string {
	var sb strings.Builder
	executor.stmt.Table.TableRefs.Left.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
	return sb.String()
}

func (executor *insertExecutor) GetInsertColumns() []string {
	result := make([]string, 0)
	for _, col := range executor.stmt.Columns {
		result = append(result, col.Name.String())
	}
	return result
}

func (executor *insertExecutor) getTableMeta(ctx context.Context) (schema.TableMeta, error) {
	dbName := executor.conn.DataSourceName()
	db := resource.GetDBManager().GetDB(dbName)
	return meta.GetTableMetaCache().GetTableMeta(ctx, db, executor.GetTableName())
}

func (executor *insertExecutor) BeforeImage(ctx context.Context) (*schema.TableRecords, error) {
	return nil, nil
}

func (executor *insertExecutor) AfterImage(ctx context.Context, result proto.Result) (*schema.TableRecords, error) {
	var afterImage *schema.TableRecords
	var err error
	pkValues, err := executor.getPKValuesByColumn(ctx)
	if err != nil {
		return nil, err
	}
	if executor.getPKIndex(ctx) >= 0 {
		afterImage, err = executor.buildTableRecords(ctx, pkValues)
	} else {
		pk, _ := result.LastInsertId()
		afterImage, err = executor.buildTableRecords(ctx, []interface{}{pk})
	}
	if err != nil {
		return nil, err
	}
	return afterImage, nil
}

func (executor *insertExecutor) buildTableRecords(ctx context.Context, pkValues []interface{}) (*schema.TableRecords, error) {
	tableMeta, err := executor.getTableMeta(ctx)
	if err != nil {
		return nil, err
	}

	afterImageSql := executor.buildAfterImageSql(tableMeta, pkValues)
	result, _, err := executor.conn.PrepareQueryArgs(afterImageSql, pkValues)
	if err != nil {
		return nil, err
	}
	return schema.BuildRecords(tableMeta, result), nil
}

func (executor *insertExecutor) buildAfterImageSql(tableMeta schema.TableMeta, pkValues []interface{}) string {
	var sb strings.Builder
	fmt.Fprint(&sb, "SELECT ")
	var i = 0
	columnCount := len(tableMeta.Columns)
	for _, column := range tableMeta.Columns {
		fmt.Fprint(&sb, misc.CheckAndReplace(column))
		i = i + 1
		if i < columnCount {
			fmt.Fprint(&sb, ",")
		} else {
			fmt.Fprint(&sb, " ")
		}
	}
	fmt.Fprintf(&sb, "FROM %s ", executor.GetTableName())
	fmt.Fprintf(&sb, " WHERE `%s` IN ", tableMeta.GetPKName())
	fmt.Fprint(&sb, appendInParam(len(pkValues)))
	return sb.String()
}

func (executor *insertExecutor) getPKValuesByColumn(ctx context.Context) ([]interface{}, error) {
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

func (executor *insertExecutor) getPKIndex(ctx context.Context) int {
	insertColumns := executor.GetInsertColumns()
	tableMeta, _ := executor.getTableMeta(ctx)

	if insertColumns != nil && len(insertColumns) > 0 {
		for i, columnName := range insertColumns {
			if strings.EqualFold(tableMeta.GetPKName(), columnName) {
				return i
			}
		}
	} else {
		allColumns := tableMeta.Columns
		var idx = 0
		for _, column := range allColumns {
			if strings.EqualFold(tableMeta.GetPKName(), column) {
				return idx
			}
			idx = idx + 1
		}
	}
	return -1
}

func (executor *insertExecutor) getColumnLen(ctx context.Context) int {
	insertColumns := executor.GetInsertColumns()
	if insertColumns != nil {
		return len(insertColumns)
	}
	tableMeta, _ := executor.getTableMeta(ctx)

	return len(tableMeta.Columns)
}

func (executor *deleteExecutor) GetTableName() string {
	var sb strings.Builder
	executor.stmt.TableRefs.TableRefs.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
	return sb.String()
}

func (executor *deleteExecutor) GetWhereCondition() string {
	var sb strings.Builder
	executor.stmt.Where.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
	return sb.String()
}

func (executor *deleteExecutor) getTableMeta(ctx context.Context) (schema.TableMeta, error) {
	dbName := executor.conn.DataSourceName()
	db := resource.GetDBManager().GetDB(dbName)
	return meta.GetTableMetaCache().GetTableMeta(ctx, db, executor.GetTableName())
}

func (executor *deleteExecutor) BeforeImage(ctx context.Context) (*schema.TableRecords, error) {
	tableMeta, err := executor.getTableMeta(ctx)
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
	return schema.BuildRecords(tableMeta, result), nil
}

func (executor *deleteExecutor) AfterImage(ctx context.Context) (*schema.TableRecords, error) {
	return nil, nil
}

func (executor *deleteExecutor) buildBeforeImageSql(tableMeta schema.TableMeta) string {
	var b strings.Builder
	fmt.Fprint(&b, "SELECT ")
	var i = 0
	columnCount := len(tableMeta.Columns)
	for _, column := range tableMeta.Columns {
		fmt.Fprint(&b, misc.CheckAndReplace(column))
		i = i + 1
		if i < columnCount {
			fmt.Fprint(&b, ",")
		} else {
			fmt.Fprint(&b, " ")
		}
	}
	fmt.Fprintf(&b, " FROM %s WHERE ", executor.GetTableName())
	fmt.Fprint(&b, executor.GetWhereCondition())
	fmt.Fprint(&b, " FOR UPDATE")
	return b.String()
}

func (executor *selectForUpdateExecutor) GetTableName() string {
	var sb strings.Builder
	table := executor.stmt.From.TableRefs.Left.(*ast.TableSource)
	table.Source.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
	return sb.String()
}

func (executor *selectForUpdateExecutor) GetWhereCondition() string {
	var sb strings.Builder
	executor.stmt.Where.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
	return sb.String()
}

func (executor *selectForUpdateExecutor) Execute(ctx context.Context, result proto.Result) (bool, error) {
	tableMeta, err := executor.getTableMeta(ctx)
	if err != nil {
		return false, err
	}

	rlt := result.(*mysql.Result)
	selectPKRows := schema.BuildRecords(tableMeta, rlt)
	lockKeys := schema.BuildLockKey(selectPKRows)
	if lockKeys == "" {
		return true, nil
	} else {
		var (
			lockable bool
			err      error
		)
		for i := 0; i < executor.lockRetryTimes; i++ {
			lockable, err = dt.GetDistributedTransactionManager().IsLockable(ctx,
				executor.conn.DataSourceName(), lockKeys)
			if lockable && err == nil {
				break
			}
			time.Sleep(executor.lockRetryInterval)
		}
		if err != nil {
			return false, err
		}
	}
	return true, err
}

func (executor *selectForUpdateExecutor) getTableMeta(ctx context.Context) (schema.TableMeta, error) {
	dbName := executor.conn.DataSourceName()
	db := resource.GetDBManager().GetDB(dbName)
	return meta.GetTableMetaCache().GetTableMeta(ctx, db, executor.GetTableName())
}

func (executor *updateExecutor) GetTableName() string {
	var sb strings.Builder
	executor.stmt.TableRefs.TableRefs.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
	return sb.String()
}

func (executor *updateExecutor) GetUpdateColumns() []string {
	columns := make([]string, 0)

	for _, assignment := range executor.stmt.List {
		columns = append(columns, assignment.Column.Name.String())
	}
	return columns
}

func (executor *updateExecutor) GetWhereCondition() string {
	var sb strings.Builder
	executor.stmt.Where.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
	return sb.String()
}

func (executor *updateExecutor) getTableMeta(ctx context.Context) (schema.TableMeta, error) {
	dbName := executor.conn.DataSourceName()
	db := resource.GetDBManager().GetDB(dbName)
	return meta.GetTableMetaCache().GetTableMeta(ctx, db, executor.GetTableName())
}

func (executor *updateExecutor) BeforeImage(ctx context.Context) (*schema.TableRecords, error) {
	tableMeta, err := executor.getTableMeta(ctx)
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
	return schema.BuildRecords(tableMeta, result), nil
}

func (executor *updateExecutor) AfterImage(ctx context.Context, beforeImage *schema.TableRecords) (*schema.TableRecords, error) {
	if beforeImage == nil || len(beforeImage.Rows) == 0 {
		return nil, nil
	}

	tableMeta, err := executor.getTableMeta(ctx)
	if err != nil {
		return nil, err
	}

	afterImageSql := executor.buildAfterImageSql(tableMeta, beforeImage)
	var args = make([]interface{}, 0)
	for _, field := range beforeImage.PKFields() {
		args = append(args, field.Value)
	}
	result, _, err := executor.conn.PrepareQueryArgs(afterImageSql, args)
	if err != nil {
		return nil, err
	}
	return schema.BuildRecords(tableMeta, result), nil
}

func (executor *updateExecutor) buildAfterImageSql(tableMeta schema.TableMeta, beforeImage *schema.TableRecords) string {
	var b strings.Builder
	fmt.Fprint(&b, "SELECT ")
	var i = 0
	columnCount := len(tableMeta.Columns)
	for _, columnName := range tableMeta.Columns {
		fmt.Fprint(&b, misc.CheckAndReplace(columnName))
		i = i + 1
		if i < columnCount {
			fmt.Fprint(&b, ",")
		} else {
			fmt.Fprint(&b, " ")
		}
	}
	fmt.Fprintf(&b, " FROM %s ", executor.GetTableName())
	fmt.Fprintf(&b, "WHERE `%s` IN", tableMeta.GetPKName())
	fmt.Fprint(&b, appendInParam(len(beforeImage.PKFields())))
	return b.String()
}

func (executor *updateExecutor) buildBeforeImageSql(tableMeta schema.TableMeta) string {
	var b strings.Builder
	fmt.Fprint(&b, "SELECT ")
	var i = 0
	columnCount := len(tableMeta.Columns)
	for _, column := range tableMeta.Columns {
		fmt.Fprint(&b, misc.CheckAndReplace(column))
		i = i + 1
		if i != columnCount {
			fmt.Fprint(&b, ",")
		} else {
			fmt.Fprint(&b, " ")
		}
	}
	fmt.Fprintf(&b, " FROM %s WHERE ", executor.GetTableName())
	fmt.Fprint(&b, executor.GetWhereCondition())
	fmt.Fprint(&b, " FOR UPDATE")
	return b.String()
}

func (executor *globalLockExecutor) GetTableName() string {
	var sb strings.Builder
	if executor.isUpdate {
		executor.updateStmt.TableRefs.TableRefs.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
	} else {
		executor.deleteStmt.TableRefs.TableRefs.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
	}
	return sb.String()
}

func (executor *globalLockExecutor) GetWhereCondition() string {
	var sb strings.Builder
	if executor.isUpdate {
		executor.updateStmt.Where.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
	} else {
		executor.deleteStmt.Where.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
	}
	return sb.String()
}

func (executor *globalLockExecutor) getTableMeta(ctx context.Context) (schema.TableMeta, error) {
	dbName := executor.conn.DataSourceName()
	db := resource.GetDBManager().GetDB(dbName)
	return meta.GetTableMetaCache().GetTableMeta(ctx, db, executor.GetTableName())
}

func (executor *globalLockExecutor) BeforeImage(ctx context.Context) (*schema.TableRecords, error) {
	tableMeta, err := executor.getTableMeta(ctx)
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
	return schema.BuildRecords(tableMeta, result), nil
}

func (executor *globalLockExecutor) buildBeforeImageSql(tableMeta schema.TableMeta) string {
	var b strings.Builder
	fmt.Fprint(&b, "SELECT ")
	var i = 0
	columnCount := len(tableMeta.Columns)
	for _, column := range tableMeta.Columns {
		fmt.Fprint(&b, misc.CheckAndReplace(column))
		i = i + 1
		if i < columnCount {
			fmt.Fprint(&b, ",")
		} else {
			fmt.Fprint(&b, " ")
		}
	}
	fmt.Fprintf(&b, " FROM %s WHERE ", executor.GetTableName())
	fmt.Fprint(&b, executor.GetWhereCondition())
	return b.String()
}

func (executor *globalLockExecutor) Executable(ctx context.Context, lockRetryInterval time.Duration, lockRetryTimes int) (bool, error) {
	beforeImage, err := executor.BeforeImage(ctx)
	if err != nil {
		return false, err
	}

	lockKeys := schema.BuildLockKey(beforeImage)
	if lockKeys == "" {
		return true, nil
	} else {
		var (
			err      error
			lockable bool
		)
		for i := 0; i < lockRetryTimes; i++ {
			lockable, err = dt.GetDistributedTransactionManager().IsLockable(ctx,
				executor.conn.DataSourceName(), lockKeys)
			if err != nil {
				time.Sleep(lockRetryInterval)
			}
			if lockable {
				return true, nil
			}
		}
		return false, err
	}
}

func appendInParam(size int) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "(")
	for i := 0; i < size; i++ {
		fmt.Fprintf(&sb, "?")
		if i < size-1 {
			fmt.Fprint(&sb, ",")
		}
	}
	fmt.Fprintf(&sb, ")")
	return sb.String()
}

func buildUndoItem(sqlType constant.SQLType, schemaName, tableName, lockKey string, beforeImage, afterImage *schema.TableRecords) *undolog.SqlUndoLog {
	sqlUndoLog := &undolog.SqlUndoLog{
		SqlType:     sqlType,
		SchemaName:  schemaName,
		TableName:   tableName,
		LockKey:     lockKey,
		BeforeImage: beforeImage,
		AfterImage:  afterImage,
	}
	return sqlUndoLog
}
