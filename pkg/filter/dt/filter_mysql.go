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
	"encoding/json"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/cectc/dbpack/pkg/constant"
	"github.com/cectc/dbpack/pkg/driver"
	"github.com/cectc/dbpack/pkg/dt"
	"github.com/cectc/dbpack/pkg/dt/api"
	"github.com/cectc/dbpack/pkg/dt/schema"
	err2 "github.com/cectc/dbpack/pkg/errors"
	"github.com/cectc/dbpack/pkg/filter"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/third_party/parser/ast"
	"github.com/cectc/dbpack/third_party/parser/model"
)

const (
	mysqlFilter    = "MysqlDistributedTransaction"
	beforeImage    = "BeforeImage"
	XID            = "x_dbpack_xid"
	BranchID       = "x_dbpack_branch_id"
	hintXID        = "XID"
	hintGlobalLock = "GlobalLock"
)

func init() {
	filter.RegistryFilterFactory(mysqlFilter, &mysqlFactory{})
}

type mysqlFactory struct {
}

func (factory *mysqlFactory) NewFilter(config map[string]interface{}) (proto.Filter, error) {
	var (
		err     error
		content []byte
	)

	if content, err = json.Marshal(config); err != nil {
		return nil, errors.Wrap(err, "marshal mysql distributed transaction filter config failed.")
	}

	v := &struct {
		ApplicationID        string        `yaml:"appid" json:"appid"`
		LockRetryInterval    time.Duration `yaml:"lock_retry_interval" json:"-"`
		LockRetryIntervalStr string        `yaml:"-" json:"lock_retry_interval"`
		LockRetryTimes       int           `yaml:"lock_retry_times" json:"lock_retry_times"`
	}{}
	if err = json.Unmarshal(content, v); err != nil {
		log.Errorf("unmarshal mysql distributed transaction filter config failed, %s", err)
		return nil, err
	}
	if v.LockRetryInterval, err = time.ParseDuration(v.LockRetryIntervalStr); err != nil {
		v.LockRetryInterval = 50 * time.Millisecond
		log.Warnf("parse mysql distributed transaction filter lock_retry_interval failed, set to default 50ms, error: %v", err)
	}

	return &_mysqlFilter{
		applicationID:     v.ApplicationID,
		lockRetryInterval: v.LockRetryInterval,
		lockRetryTimes:    v.LockRetryTimes,
	}, nil
}

type _mysqlFilter struct {
	applicationID     string
	lockRetryInterval time.Duration
	lockRetryTimes    int
}

func (f *_mysqlFilter) GetName() string {
	return mysqlFilter
}

func (f *_mysqlFilter) PreHandle(ctx context.Context, conn proto.Connection) error {
	var err error
	bc := conn.(*driver.BackendConnection)
	commandType := proto.CommandType(ctx)
	if commandType == constant.ComStmtExecute {
		stmt := proto.PrepareStmt(ctx)
		if stmt == nil {
			return errors.New("prepare stmt should not be nil")
		}
		switch stmtNode := stmt.StmtNode.(type) {
		case *ast.DeleteStmt:
			err = f.processBeforeDelete(ctx, bc, stmt, stmtNode)
		case *ast.UpdateStmt:
			err = f.processBeforeUpdate(ctx, bc, stmt, stmtNode)
		default:
			return nil
		}
	}
	return err
}

func (f *_mysqlFilter) PostHandle(ctx context.Context, result proto.Result, conn proto.Connection) error {
	var err error
	bc := conn.(*driver.BackendConnection)
	commandType := proto.CommandType(ctx)
	if commandType == constant.ComStmtExecute {
		stmt := proto.PrepareStmt(ctx)
		if stmt == nil {
			return errors.New("prepare stmt should not be nil")
		}
		switch stmtNode := stmt.StmtNode.(type) {
		case *ast.DeleteStmt:
			err = f.processAfterDelete(ctx, bc, result, stmt, stmtNode)
		case *ast.InsertStmt:
			err = f.processAfterInsert(ctx, bc, result, stmt, stmtNode)
		case *ast.UpdateStmt:
			err = f.processAfterUpdate(ctx, bc, result, stmt, stmtNode)
		case *ast.SelectStmt:
			if stmtNode.LockInfo != nil && stmtNode.LockInfo.LockType == ast.SelectLockForUpdate {
				err = f.processSelectForUpdate(ctx, bc, result, stmt, stmtNode)
			}
		default:
			return nil
		}
	}
	return err
}

func (f *_mysqlFilter) processBeforeDelete(ctx context.Context, conn *driver.BackendConnection, stmt *proto.Stmt, deleteStmt *ast.DeleteStmt) error {
	if hasGlobalLockHint(deleteStmt.TableHints) {
		executor := &globalLockExecutor{
			conn:       conn,
			isUpdate:   false,
			deleteStmt: deleteStmt,
			updateStmt: nil,
		}
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
	executor := &deleteExecutor{
		conn: conn,
		stmt: deleteStmt,
		args: stmt.BindVars,
	}
	bi, err := executor.BeforeImage(ctx)
	if err != nil {
		return err
	}
	if !proto.WithVariable(ctx, beforeImage, bi) {
		return errors.New("set before image failed")
	}
	return nil
}

func (f *_mysqlFilter) processBeforeUpdate(ctx context.Context, conn *driver.BackendConnection, stmt *proto.Stmt, updateStmt *ast.UpdateStmt) error {
	if hasGlobalLockHint(updateStmt.TableHints) {
		executor := &globalLockExecutor{
			conn:       conn,
			isUpdate:   true,
			deleteStmt: nil,
			updateStmt: updateStmt,
		}
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
	executor := &updateExecutor{
		conn: conn,
		stmt: updateStmt,
		args: stmt.BindVars,
	}
	bi, err := executor.BeforeImage(ctx)
	if err != nil {
		return err
	}
	if !proto.WithVariable(ctx, beforeImage, bi) {
		return errors.New("set before image failed")
	}
	return nil
}

func (f *_mysqlFilter) processAfterDelete(ctx context.Context, conn *driver.BackendConnection,
	result proto.Result, stmt *proto.Stmt, deleteStmt *ast.DeleteStmt) error {
	has, xid := hasXIDHint(deleteStmt.TableHints)
	if !has {
		return nil
	}

	executor := &deleteExecutor{
		conn: conn,
		stmt: deleteStmt,
		args: stmt.BindVars,
	}
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
	undoLog := buildUndoItem(constant.SQLType_DELETE, schemaName, executor.GetTableName(), lockKeys, biValue, nil)

	branchID, err := f.registerBranchTransaction(ctx, xid, conn.DataSourceName(), lockKeys)
	if err != nil {
		return err
	}
	log.Debugf("delete, branch id: %d", branchID)
	return dt.GetUndoLogManager().InsertUndoLogWithNormal(conn, xid, branchID, undoLog)
}

func (f *_mysqlFilter) processAfterInsert(ctx context.Context, conn *driver.BackendConnection,
	result proto.Result, stmt *proto.Stmt, insertStmt *ast.InsertStmt) error {
	has, xid := hasXIDHint(insertStmt.TableHints)
	if !has {
		return nil
	}

	executor := &insertExecutor{
		conn: conn,
		stmt: insertStmt,
		args: stmt.BindVars,
	}
	afterImage, err := executor.AfterImage(ctx, result)
	if err != nil {
		return err
	}
	schemaName := proto.Schema(ctx)
	if schemaName == "" {
		return errors.New("schema name should not be nil")
	}

	lockKeys := schema.BuildLockKey(afterImage)
	log.Debugf("insert, lockKey: %s", lockKeys)
	undoLog := buildUndoItem(constant.SQLType_INSERT, schemaName, executor.GetTableName(), lockKeys, nil, afterImage)

	branchID, err := f.registerBranchTransaction(ctx, xid, conn.DataSourceName(), lockKeys)
	if err != nil {
		return err
	}
	log.Debugf("insert, branch id: %d", branchID)
	return dt.GetUndoLogManager().InsertUndoLogWithNormal(conn, xid, branchID, undoLog)
}

func (f *_mysqlFilter) processAfterUpdate(ctx context.Context, conn *driver.BackendConnection,
	result proto.Result, stmt *proto.Stmt, updateStmt *ast.UpdateStmt) error {
	has, xid := hasXIDHint(updateStmt.TableHints)
	if !has {
		return nil
	}

	executor := &updateExecutor{
		conn: conn,
		stmt: updateStmt,
		args: stmt.BindVars,
	}
	bi := proto.Variable(ctx, beforeImage)
	if bi == nil {
		return errors.New("before image should not be nil")
	}
	biValue := bi.(*schema.TableRecords)
	afterImage, err := executor.AfterImage(ctx, biValue)
	if err != nil {
		return err
	}
	schemaName := proto.Schema(ctx)
	if schemaName == "" {
		return errors.New("schema name should not be nil")
	}

	lockKeys := schema.BuildLockKey(afterImage)
	log.Debugf("update, lockKey: %s", lockKeys)
	undoLog := buildUndoItem(constant.SQLType_UPDATE, schemaName, executor.GetTableName(), lockKeys, biValue, afterImage)

	branchID, err := f.registerBranchTransaction(ctx, xid, conn.DataSourceName(), lockKeys)
	if err != nil {
		return err
	}
	log.Debugf("update, branch id: %d", branchID)
	return dt.GetUndoLogManager().InsertUndoLogWithNormal(conn, xid, branchID, undoLog)
}

func (f *_mysqlFilter) processSelectForUpdate(ctx context.Context, conn *driver.BackendConnection,
	result proto.Result, stmt *proto.Stmt, selectStmt *ast.SelectStmt) error {
	has, _ := hasXIDHint(selectStmt.TableHints)
	if !has {
		return nil
	}
	executor := &selectForUpdateExecutor{
		conn:              conn,
		stmt:              selectStmt,
		args:              stmt.BindVars,
		lockRetryInterval: f.lockRetryInterval,
		lockRetryTimes:    f.lockRetryTimes,
	}
	_, err := executor.Execute(ctx, result)
	return err
}

func (f *_mysqlFilter) registerBranchTransaction(ctx context.Context, xid, resourceID, lockKey string) (int64, error) {
	var (
		branchID int64
		err      error
	)
	br := &api.BranchRegisterRequest{
		XID:             xid,
		ResourceID:      resourceID,
		LockKey:         lockKey,
		BranchType:      api.AT,
		ApplicationData: nil,
	}
	for retryCount := 0; retryCount < f.lockRetryTimes; retryCount++ {
		_, branchID, err = dt.GetDistributedTransactionManager().BranchRegister(context.Background(), br)
		if err == nil {
			break
		}
		log.Errorf("branch register err: %v", err)
		if errors.Is(err, err2.BranchLockAcquireFailed) {
			time.Sleep(f.lockRetryInterval)
			continue
		} else {
			break
		}
	}
	return branchID, err
}

func hasXIDHint(hints []*ast.TableOptimizerHint) (bool, string) {
	for _, hint := range hints {
		if strings.EqualFold(hint.HintName.String(), hintXID) {
			hintData := hint.HintData.(model.CIStr)
			xid := hintData.String()
			return true, xid
		}
	}
	return false, ""
}

func hasGlobalLockHint(hints []*ast.TableOptimizerHint) bool {
	for _, hint := range hints {
		if strings.EqualFold(hint.HintName.String(), hintGlobalLock) {
			return true
		}
	}
	return false
}
