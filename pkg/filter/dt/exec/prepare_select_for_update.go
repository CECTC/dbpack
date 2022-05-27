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
	"strings"
	"time"

	"github.com/cectc/dbpack/pkg/driver"
	"github.com/cectc/dbpack/pkg/dt"
	"github.com/cectc/dbpack/pkg/dt/schema"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/meta"
	"github.com/cectc/dbpack/pkg/mysql"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/pkg/resource"
	"github.com/cectc/dbpack/third_party/parser/ast"
	"github.com/cectc/dbpack/third_party/parser/format"
)

type prepareSelectForUpdateExecutor struct {
	conn   *driver.BackendConnection
	stmt   *ast.SelectStmt
	args   map[string]interface{}
	result proto.Result
}

func NewPrepareSelectForUpdateExecutor(
	conn *driver.BackendConnection,
	stmt *ast.SelectStmt,
	args map[string]interface{},
	result proto.Result) Executable {
	return &prepareSelectForUpdateExecutor{
		conn:   conn,
		stmt:   stmt,
		args:   args,
		result: result,
	}
}

func (executor *prepareSelectForUpdateExecutor) Executable(ctx context.Context, lockRetryInterval time.Duration, lockRetryTimes int) (bool, error) {
	tableMeta, err := executor.GetTableMeta(ctx)
	if err != nil {
		return false, err
	}

	rlt := executor.result.(*mysql.Result)
	selectPKRows := schema.BuildBinaryRecords(tableMeta, rlt)
	lockKeys := schema.BuildLockKey(selectPKRows)
	if lockKeys == "" {
		return true, nil
	} else {
		var (
			lockable bool
			err      error
		)
		for i := 0; i < lockRetryTimes; i++ {
			lockable, err = dt.GetDistributedTransactionManager().IsLockable(ctx,
				executor.conn.DataSourceName(), lockKeys)
			if lockable && err == nil {
				break
			}
			time.Sleep(lockRetryInterval)
		}
		if err != nil {
			return false, err
		}
	}
	return true, err
}

func (executor *prepareSelectForUpdateExecutor) GetTableMeta(ctx context.Context) (schema.TableMeta, error) {
	dbName := executor.conn.DataSourceName()
	db := resource.GetDBManager().GetDB(dbName)
	return meta.GetTableMetaCache().GetTableMeta(ctx, db, executor.GetTableName())
}

func (executor *prepareSelectForUpdateExecutor) GetTableName() string {
	var sb strings.Builder
	table := executor.stmt.From.TableRefs.Left.(*ast.TableSource)
	if err := table.Source.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
		log.Panic(err)
	}
	return sb.String()
}

func (executor *prepareSelectForUpdateExecutor) GetWhereCondition() string {
	var sb strings.Builder
	executor.stmt.Where.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
	return sb.String()
}
