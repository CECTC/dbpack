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
	"time"

	"github.com/cectc/dbpack/pkg/driver"
	"github.com/cectc/dbpack/pkg/dt"
	"github.com/cectc/dbpack/pkg/dt/schema"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/meta"
	"github.com/cectc/dbpack/pkg/misc"
	"github.com/cectc/dbpack/pkg/resource"
	"github.com/cectc/dbpack/third_party/parser/ast"
	"github.com/cectc/dbpack/third_party/parser/format"
)

type prepareGlobalLockExecutor struct {
	conn       *driver.BackendConnection
	isUpdate   bool
	deleteStmt *ast.DeleteStmt
	updateStmt *ast.UpdateStmt
	args       map[string]interface{}
}

func NewPrepareGlobalLockExecutor(
	conn *driver.BackendConnection,
	isUpdate bool,
	deleteStmt *ast.DeleteStmt,
	updateStmt *ast.UpdateStmt,
	args map[string]interface{}) Executable {
	return &prepareGlobalLockExecutor{
		conn:       conn,
		isUpdate:   isUpdate,
		deleteStmt: deleteStmt,
		updateStmt: updateStmt,
		args:       args,
	}
}

func (executor *prepareGlobalLockExecutor) Executable(ctx context.Context, lockRetryInterval time.Duration, lockRetryTimes int) (bool, error) {
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

func (executor *prepareGlobalLockExecutor) GetTableMeta(ctx context.Context) (schema.TableMeta, error) {
	dbName := executor.conn.DataSourceName()
	db := resource.GetDBManager().GetDB(dbName)
	return meta.GetTableMetaCache().GetTableMeta(ctx, db, executor.GetTableName())
}

func (executor *prepareGlobalLockExecutor) GetTableName() string {
	var sb strings.Builder
	if executor.isUpdate {
		if err := executor.updateStmt.TableRefs.TableRefs.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
			log.Panic(err)
		}
	} else {
		if err := executor.deleteStmt.TableRefs.TableRefs.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
			log.Panic(err)
		}
	}
	return sb.String()
}

func (executor *prepareGlobalLockExecutor) BeforeImage(ctx context.Context) (*schema.TableRecords, error) {
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

func (executor *prepareGlobalLockExecutor) buildBeforeImageSql(tableMeta schema.TableMeta) string {
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
	b.WriteString(fmt.Sprintf(" FROM %s WHERE ", executor.GetTableName()))
	b.WriteString(executor.GetWhereCondition())
	return b.String()
}

func (executor *prepareGlobalLockExecutor) GetWhereCondition() string {
	var sb strings.Builder
	if executor.isUpdate {
		if err := executor.updateStmt.Where.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
			log.Panic(err)
		}
	} else {
		if err := executor.deleteStmt.Where.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
			log.Panic(err)
		}
	}
	return sb.String()
}
