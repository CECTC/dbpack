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

	"github.com/pkg/errors"

	"github.com/cectc/dbpack/pkg/constant"
	"github.com/cectc/dbpack/pkg/driver"
	"github.com/cectc/dbpack/pkg/dt"
	"github.com/cectc/dbpack/pkg/dt/schema"
	"github.com/cectc/dbpack/pkg/filter/dt/exec"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/third_party/parser/ast"
)

func (f *_mysqlFilter) processBeforePrepareDelete(ctx context.Context, conn *driver.BackendConnection, stmt *proto.Stmt, deleteStmt *ast.DeleteStmt) error {
	if hasGlobalLockHint(deleteStmt.TableHints) {
		executor := exec.NewPrepareGlobalLockExecutor(conn, false, deleteStmt, nil, stmt.BindVars)
		result, err := executor.Executable(ctx, f.lockRetryInterval, f.lockRetryTimes)
		if err != nil {
			return err
		}
		if !result {
			return errors.New("resource locked by distributed transaction global lock!")
		}
		return nil
	}
	if has, _ := hasXIDHint(deleteStmt.TableHints); !has {
		return nil
	}
	executor := exec.NewPrepareDeleteExecutor(conn, deleteStmt, stmt.BindVars)
	bi, err := executor.BeforeImage(ctx)
	if err != nil {
		return err
	}
	if !proto.WithVariable(ctx, beforeImage, bi) {
		return errors.New("set before image failed")
	}
	return nil
}

func (f *_mysqlFilter) processBeforePrepareUpdate(ctx context.Context, conn *driver.BackendConnection, stmt *proto.Stmt, updateStmt *ast.UpdateStmt) error {
	if hasGlobalLockHint(updateStmt.TableHints) {
		executor := exec.NewPrepareGlobalLockExecutor(conn, true, nil, updateStmt, stmt.BindVars)
		result, err := executor.Executable(ctx, f.lockRetryInterval, f.lockRetryTimes)
		if err != nil {
			return err
		}
		if !result {
			return errors.New("resource locked by distributed transaction global lock!")
		}
		return nil
	}
	if has, _ := hasXIDHint(updateStmt.TableHints); !has {
		return nil
	}
	executor := exec.NewPrepareUpdateExecutor(conn, updateStmt, stmt.BindVars, nil)
	bi, err := executor.BeforeImage(ctx)
	if err != nil {
		return err
	}
	if !proto.WithVariable(ctx, beforeImage, bi) {
		return errors.New("set before image failed")
	}
	return nil
}

func (f *_mysqlFilter) processAfterPrepareDelete(ctx context.Context, conn *driver.BackendConnection,
	stmt *proto.Stmt, deleteStmt *ast.DeleteStmt) error {
	has, xid := hasXIDHint(deleteStmt.TableHints)
	if !has {
		return nil
	}

	executor := exec.NewPrepareDeleteExecutor(conn, deleteStmt, stmt.BindVars)
	bi := proto.Variable(ctx, beforeImage)
	if bi == nil {
		return errors.New("before image should not be nil")
	}
	biValue := bi.(*schema.TableRecords)
	schemaName := proto.Schema(ctx)
	if schemaName == "" {
		return errors.New("schema name should not be nil")
	}

	lockKeys := schema.BuildLockKey(biValue)
	log.Debugf("delete, lockKey: %s", lockKeys)
	undoLog := exec.BuildUndoItem(true, constant.SQLType_DELETE, schemaName, executor.GetTableName(), lockKeys, biValue, nil)

	branchID, err := f.registerBranchTransaction(ctx, xid, conn.DataSourceName(), lockKeys)
	if err != nil {
		return err
	}
	log.Debugf("delete, branch id: %d", branchID)
	return dt.GetUndoLogManager().InsertUndoLogWithNormal(conn, xid, branchID, undoLog)
}

func (f *_mysqlFilter) processAfterPrepareInsert(ctx context.Context, conn *driver.BackendConnection,
	result proto.Result, stmt *proto.Stmt, insertStmt *ast.InsertStmt) error {
	has, xid := hasXIDHint(insertStmt.TableHints)
	if !has {
		return nil
	}

	executor := exec.NewPrepareInsertExecutor(conn, insertStmt, stmt.BindVars, result)
	afterImage, err := executor.AfterImage(ctx)
	if err != nil {
		return err
	}
	schemaName := proto.Schema(ctx)
	if schemaName == "" {
		return errors.New("schema name should not be nil")
	}

	lockKeys := schema.BuildLockKey(afterImage)
	log.Debugf("insert, lockKey: %s", lockKeys)
	undoLog := exec.BuildUndoItem(true, constant.SQLType_INSERT, schemaName, executor.GetTableName(), lockKeys, nil, afterImage)

	branchID, err := f.registerBranchTransaction(ctx, xid, conn.DataSourceName(), lockKeys)
	if err != nil {
		return err
	}
	log.Debugf("insert, branch id: %d", branchID)
	return dt.GetUndoLogManager().InsertUndoLogWithNormal(conn, xid, branchID, undoLog)
}

func (f *_mysqlFilter) processAfterPrepareUpdate(ctx context.Context, conn *driver.BackendConnection,
	stmt *proto.Stmt, updateStmt *ast.UpdateStmt) error {
	has, xid := hasXIDHint(updateStmt.TableHints)
	if !has {
		return nil
	}
	bi := proto.Variable(ctx, beforeImage)
	if bi == nil {
		return errors.New("before image should not be nil")
	}
	beforeImage := bi.(*schema.TableRecords)
	executor := exec.NewPrepareUpdateExecutor(conn, updateStmt, stmt.BindVars, beforeImage)
	afterImage, err := executor.AfterImage(ctx)
	if err != nil {
		return err
	}
	schemaName := proto.Schema(ctx)
	if schemaName == "" {
		return errors.New("schema name should not be nil")
	}

	lockKeys := schema.BuildLockKey(afterImage)
	log.Debugf("update, lockKey: %s", lockKeys)
	undoLog := exec.BuildUndoItem(true, constant.SQLType_UPDATE, schemaName, executor.GetTableName(), lockKeys, beforeImage, afterImage)

	branchID, err := f.registerBranchTransaction(ctx, xid, conn.DataSourceName(), lockKeys)
	if err != nil {
		return err
	}
	log.Debugf("update, branch id: %d", branchID)
	return dt.GetUndoLogManager().InsertUndoLogWithNormal(conn, xid, branchID, undoLog)
}

func (f *_mysqlFilter) processSelectForPrepareUpdate(ctx context.Context, conn *driver.BackendConnection,
	result proto.Result, stmt *proto.Stmt, selectStmt *ast.SelectStmt) error {
	has, _ := hasXIDHint(selectStmt.TableHints)
	if !has {
		return nil
	}
	executor := exec.NewPrepareSelectForUpdateExecutor(conn, selectStmt, stmt.BindVars, result)
	_, err := executor.Executable(ctx, f.lockRetryInterval, f.lockRetryTimes)
	return err
}
