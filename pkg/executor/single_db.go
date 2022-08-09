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

package executor

import (
	"context"
	"encoding/json"
	"strings"
	"sync"

	"github.com/pkg/errors"

	"github.com/cectc/dbpack/pkg/config"
	"github.com/cectc/dbpack/pkg/filter"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/pkg/resource"
	"github.com/cectc/dbpack/pkg/tracing"
	"github.com/cectc/dbpack/third_party/parser/ast"
	"github.com/cectc/dbpack/third_party/parser/format"
)

type SingleDBExecutor struct {
	conf        *config.Executor
	PreFilters  []proto.DBPreFilter
	PostFilters []proto.DBPostFilter

	dataSource string
	// map[uint32]proto.Tx
	localTransactionMap *sync.Map
}

func NewSingleDBExecutor(conf *config.Executor) (proto.Executor, error) {
	var (
		err     error
		content []byte
	)

	if content, err = json.Marshal(conf.Config); err != nil {
		return nil, errors.Wrap(err, "marshal single db executor datasource config failed.")
	}

	v := &struct {
		DataSource string `yaml:"data_source_ref" json:"data_source_ref"`
	}{}

	if err = json.Unmarshal(content, v); err != nil {
		log.Errorf("unmarshal single db executor datasource config failed, %s", err)
		return nil, err
	}

	executor := &SingleDBExecutor{
		conf:                conf,
		PreFilters:          make([]proto.DBPreFilter, 0),
		PostFilters:         make([]proto.DBPostFilter, 0),
		dataSource:          v.DataSource,
		localTransactionMap: &sync.Map{},
	}

	for i := 0; i < len(conf.Filters); i++ {
		filterName := conf.Filters[i]
		f := filter.GetFilter(conf.AppID, filterName)
		if f != nil {
			preFilter, ok := f.(proto.DBPreFilter)
			if ok {
				executor.PreFilters = append(executor.PreFilters, preFilter)
			}
			postFilter, ok := f.(proto.DBPostFilter)
			if ok {
				executor.PostFilters = append(executor.PostFilters, postFilter)
			}
		}
	}

	return executor, nil
}

func (executor *SingleDBExecutor) GetPreFilters() []proto.DBPreFilter {
	return executor.PreFilters
}

func (executor *SingleDBExecutor) GetPostFilters() []proto.DBPostFilter {
	return executor.PostFilters
}

func (executor *SingleDBExecutor) ExecuteMode() config.ExecuteMode {
	return config.SDB
}

func (executor *SingleDBExecutor) ProcessDistributedTransaction() bool {
	return false
}

func (executor *SingleDBExecutor) InLocalTransaction(ctx context.Context) bool {
	connectionID := proto.ConnectionID(ctx)
	_, ok := executor.localTransactionMap.Load(connectionID)
	return ok
}

func (executor *SingleDBExecutor) InGlobalTransaction(ctx context.Context) bool {
	return false
}

func (executor *SingleDBExecutor) ExecuteUseDB(ctx context.Context, schema string) error {
	db := resource.GetDBManager(executor.conf.AppID).GetDB(executor.dataSource)
	return db.UseDB(ctx, schema)
}

func (executor *SingleDBExecutor) ExecuteFieldList(ctx context.Context, table, wildcard string) ([]proto.Field, error) {
	db := resource.GetDBManager(executor.conf.AppID).GetDB(executor.dataSource)
	return db.ExecuteFieldList(ctx, table, wildcard)
}

func (executor *SingleDBExecutor) ExecutorComQuery(
	ctx context.Context, sqlText string) (result proto.Result, warns uint16, err error) {
	spanCtx, span := tracing.GetTraceSpan(ctx, tracing.SDBComQuery)
	defer span.End()

	if err = executor.doPreFilter(spanCtx); err != nil {
		return nil, 0, err
	}
	defer func() {
		if err == nil {
			result, err = decodeTextResult(result)
			if err != nil {
				span.RecordError(err)
				return
			}
			err = executor.doPostFilter(spanCtx, result)
		} else {
			span.RecordError(err)
		}
	}()

	var (
		db proto.DB
		tx proto.Tx
		sb strings.Builder
	)

	connectionID := proto.ConnectionID(spanCtx)
	queryStmt := proto.QueryStmt(spanCtx)
	if queryStmt == nil {
		return nil, 0, errors.New("query stmt should not be nil")
	}
	if err := queryStmt.Restore(format.NewRestoreCtx(
		format.DefaultRestoreFlags|
			format.RestoreStringWithoutDefaultCharset, &sb)); err != nil {
		return nil, 0, err
	}
	sql := sb.String()
	spanCtx = proto.WithSqlText(spanCtx, sql)

	log.Debugf("connectionID: %d, query: %s", connectionID, sql)
	db = resource.GetDBManager(executor.conf.AppID).GetDB(executor.dataSource)
	switch stmt := queryStmt.(type) {
	case *ast.SetStmt:
		if shouldStartTransaction(stmt) {
			// TODO add metrics
			tx, result, err = db.Begin(spanCtx)
			if err != nil {
				return nil, 0, err
			}
			executor.localTransactionMap.Store(connectionID, tx)
			return result, 0, nil
		} else {
			txi, ok := executor.localTransactionMap.Load(connectionID)
			if ok {
				tx = txi.(proto.Tx)
				return tx.Query(spanCtx, sqlText)
			}
			return db.Query(spanCtx, sqlText)
		}
	case *ast.BeginStmt:
		// TODO add metrics
		tx, result, err = db.Begin(spanCtx)
		if err != nil {
			return nil, 0, err
		}
		executor.localTransactionMap.Store(connectionID, tx)
		return result, 0, nil
	case *ast.CommitStmt:
		txi, ok := executor.localTransactionMap.Load(connectionID)
		if !ok {
			return nil, 0, errors.New("there is no transaction")
		}
		defer executor.localTransactionMap.Delete(connectionID)
		tx = txi.(proto.Tx)
		// TODO add metrics
		if result, err = tx.Commit(spanCtx); err != nil {
			return nil, 0, err
		}
		return result, 0, err
	case *ast.RollbackStmt:
		txi, ok := executor.localTransactionMap.Load(connectionID)
		if !ok {
			return nil, 0, errors.New("there is no transaction")
		}
		defer executor.localTransactionMap.Delete(connectionID)
		tx = txi.(proto.Tx)
		// TODO add metrics
		if result, err = tx.Rollback(spanCtx, stmt); err != nil {
			return nil, 0, err
		}
		return result, 0, err
	default:
		txi, ok := executor.localTransactionMap.Load(connectionID)
		if ok {
			tx = txi.(proto.Tx)
			return tx.Query(spanCtx, sql)
		}
		return db.Query(spanCtx, sql)
	}
}

func (executor *SingleDBExecutor) ExecutorComStmtExecute(
	ctx context.Context, stmt *proto.Stmt) (result proto.Result, warns uint16, err error) {
	spanCtx, span := tracing.GetTraceSpan(ctx, tracing.SDBComStmtExecute)
	defer span.End()

	if err = executor.doPreFilter(spanCtx); err != nil {
		return nil, 0, err
	}
	defer func() {
		if err == nil {
			result, err = decodeBinaryResult(result)
			if err != nil {
				span.RecordError(err)
				return
			}
			err = executor.doPostFilter(spanCtx, result)
		} else {
			span.RecordError(err)
		}
	}()

	connectionID := proto.ConnectionID(ctx)
	log.Debugf("connectionID: %d, prepare: %s", connectionID, stmt.SqlText)
	txi, ok := executor.localTransactionMap.Load(connectionID)
	if ok {
		tx := txi.(proto.Tx)
		return tx.ExecuteStmt(spanCtx, stmt)
	}
	db := resource.GetDBManager(executor.conf.AppID).GetDB(executor.dataSource)
	return db.ExecuteStmt(spanCtx, stmt)
}

func (executor *SingleDBExecutor) ConnectionClose(ctx context.Context) {
	connectionID := proto.ConnectionID(ctx)
	txi, ok := executor.localTransactionMap.Load(connectionID)
	if !ok {
		return
	}
	tx := txi.(proto.Tx)
	if _, err := tx.Rollback(ctx, nil); err != nil {
		log.Error(err)
	}
	executor.localTransactionMap.Delete(connectionID)
}

func (executor *SingleDBExecutor) doPreFilter(ctx context.Context) error {
	for i := 0; i < len(executor.PreFilters); i++ {
		f := executor.PreFilters[i]
		err := f.PreHandle(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (executor *SingleDBExecutor) doPostFilter(ctx context.Context, result proto.Result) error {
	for i := 0; i < len(executor.PostFilters); i++ {
		f := executor.PostFilters[i]
		err := f.PostHandle(ctx, result)
		if err != nil {
			return err
		}
	}
	return nil
}
