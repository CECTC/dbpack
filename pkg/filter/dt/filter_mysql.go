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
	XID            = "x-dbpack-xid"
	BranchID       = "x-dbpack-branch-id"
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

func (f *_mysqlFilter) GetKind() string {
	return mysqlFilter
}

func (f *_mysqlFilter) PreHandle(ctx context.Context, conn proto.Connection) error {
	var err error
	bc := conn.(*driver.BackendConnection)
	commandType := proto.CommandType(ctx)
	switch commandType {
	case constant.ComQuery:
		stmt := proto.QueryStmt(ctx)
		if stmt == nil {
			return errors.New("query stmt should not be nil")
		}
		switch stmtNode := stmt.(type) {
		case *ast.DeleteStmt:
			err = f.processBeforeQueryDelete(ctx, bc, stmtNode)
		case *ast.UpdateStmt:
			err = f.processBeforeQueryUpdate(ctx, bc, stmtNode)
		default:
			return nil
		}
	case constant.ComStmtExecute:
		stmt := proto.PrepareStmt(ctx)
		if stmt == nil {
			return errors.New("prepare stmt should not be nil")
		}
		switch stmtNode := stmt.StmtNode.(type) {
		case *ast.DeleteStmt:
			err = f.processBeforePrepareDelete(ctx, bc, stmt, stmtNode)
		case *ast.UpdateStmt:
			err = f.processBeforePrepareUpdate(ctx, bc, stmt, stmtNode)
		default:
			return nil
		}
	default:
		return nil
	}
	return err
}

func (f *_mysqlFilter) PostHandle(ctx context.Context, result proto.Result, conn proto.Connection) error {
	var err error
	bc := conn.(*driver.BackendConnection)
	commandType := proto.CommandType(ctx)
	switch commandType {
	case constant.ComQuery:
		stmt := proto.QueryStmt(ctx)
		if stmt == nil {
			return errors.New("query stmt should not be nil")
		}
		switch stmtNode := stmt.(type) {
		case *ast.DeleteStmt:
			err = f.processAfterQueryDelete(ctx, bc, stmtNode)
		case *ast.InsertStmt:
			err = f.processAfterQueryInsert(ctx, bc, result, stmtNode)
		case *ast.UpdateStmt:
			err = f.processAfterQueryUpdate(ctx, bc, stmtNode)
		case *ast.SelectStmt:
			if stmtNode.LockInfo != nil && stmtNode.LockInfo.LockType == ast.SelectLockForUpdate {
				err = f.processSelectForQueryUpdate(ctx, bc, result, stmtNode)
			}
		default:
			return nil
		}
	case constant.ComStmtExecute:
		stmt := proto.PrepareStmt(ctx)
		if stmt == nil {
			return errors.New("prepare stmt should not be nil")
		}
		switch stmtNode := stmt.StmtNode.(type) {
		case *ast.DeleteStmt:
			err = f.processAfterPrepareDelete(ctx, bc, stmt, stmtNode)
		case *ast.InsertStmt:
			err = f.processAfterPrepareInsert(ctx, bc, result, stmt, stmtNode)
		case *ast.UpdateStmt:
			err = f.processAfterPrepareUpdate(ctx, bc, stmt, stmtNode)
		case *ast.SelectStmt:
			if stmtNode.LockInfo != nil && stmtNode.LockInfo.LockType == ast.SelectLockForUpdate {
				err = f.processSelectForPrepareUpdate(ctx, bc, result, stmt, stmtNode)
			}
		default:
			return nil
		}
	default:
		return nil
	}
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
