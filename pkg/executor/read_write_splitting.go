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
	"sync"

	"github.com/pkg/errors"

	"github.com/cectc/dbpack/pkg/config"
	"github.com/cectc/dbpack/pkg/filter"
	"github.com/cectc/dbpack/pkg/lb"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/mysql"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/third_party/parser/ast"
)

type ReadWriteSplittingExecutor struct {
	PreFilters  []proto.DBPreFilter
	PostFilters []proto.DBPostFilter

	all     []*DataSourceBrief
	masters lb.Interface
	reads   lb.Interface
	// map[uint32]proto.Tx
	localTransactionMap *sync.Map
}

func NewReadWriteSplittingExecutor(conf *config.Executor) (proto.Executor, error) {
	var (
		err      error
		content  []byte
		rwConfig *config.ReadWriteSplittingConfig
	)

	if content, err = json.Marshal(conf.Config); err != nil {
		return nil, errors.Wrap(err, "marshal read write splitting executor datasource config failed.")
	}

	if err = json.Unmarshal(content, &rwConfig); err != nil {
		log.Errorf("unmarshal read write splitting executor datasource config failed, %s", err)
		return nil, err
	}

	all, masters, reads, err := groupDataSourceRefs(rwConfig.DataSources)
	if err != nil {
		return nil, err
	}

	masterLB, err := lb.New(rwConfig.LoadBalanceAlgorithm)
	if err != nil {
		return nil, err
	}
	readLB, err := lb.New(rwConfig.LoadBalanceAlgorithm)
	if err != nil {
		return nil, err
	}
	for _, master := range masters {
		masterLB.Add(master)
	}
	for _, read := range reads {
		readLB.Add(read)
	}

	executor := &ReadWriteSplittingExecutor{
		PreFilters:          make([]proto.DBPreFilter, 0),
		PostFilters:         make([]proto.DBPostFilter, 0),
		all:                 all,
		masters:             masterLB,
		reads:               readLB,
		localTransactionMap: &sync.Map{},
	}

	for i := 0; i < len(conf.Filters); i++ {
		filterName := conf.Filters[i]
		f := filter.GetFilter(filterName)
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

func (executor *ReadWriteSplittingExecutor) GetPreFilters() []proto.DBPreFilter {
	return executor.PreFilters
}

func (executor *ReadWriteSplittingExecutor) GetPostFilters() []proto.DBPostFilter {
	return executor.PostFilters
}

func (executor *ReadWriteSplittingExecutor) ExecuteMode() config.ExecuteMode {
	return config.RWS
}

func (executor *ReadWriteSplittingExecutor) ProcessDistributedTransaction() bool {
	return false
}

func (executor *ReadWriteSplittingExecutor) InLocalTransaction(ctx context.Context) bool {
	connectionID := proto.ConnectionID(ctx)
	_, ok := executor.localTransactionMap.Load(connectionID)
	return ok
}

func (executor *ReadWriteSplittingExecutor) InGlobalTransaction(ctx context.Context) bool {
	return false
}

func (executor *ReadWriteSplittingExecutor) ExecuteUseDB(ctx context.Context, db string) error {
	return errors.New("unimplemented COM_INIT_DB in read write splitting mode")
}

// ExecuteFieldList
// https://dev.mysql.com/doc/internals/en/com-field-list.html
// As of MySQL 5.7.11, COM_FIELD_LIST is deprecated and will be removed in a future version of MySQL.
// Instead, use mysql_query() to execute a SHOW COLUMNS statement.
func (executor *ReadWriteSplittingExecutor) ExecuteFieldList(ctx context.Context, table, wildcard string) ([]proto.Field, error) {
	return nil, errors.New("unimplemented COM_FIELD_LIST in read write splitting mode")
}

func (executor *ReadWriteSplittingExecutor) ExecutorComQuery(ctx context.Context, sql string) (proto.Result, uint16, error) {
	var (
		db     *DataSourceBrief
		tx     proto.Tx
		result proto.Result
		err    error
	)

	connectionID := proto.ConnectionID(ctx)
	queryStmt := proto.QueryStmt(ctx)

	switch stmt := queryStmt.(type) {
	case *ast.SetStmt:
		if shouldStartTransaction(stmt) {
			db = executor.masters.Next(proto.WithMaster(ctx)).(*DataSourceBrief)
			// TODO add metrics
			tx, result, err = db.DB.Begin(ctx)
			if err != nil {
				return nil, 0, err
			}
			executor.localTransactionMap.Store(connectionID, tx)
			return result, 0, nil
		} else {
			txi, ok := executor.localTransactionMap.Load(connectionID)
			if ok {
				tx = txi.(proto.Tx)
				return tx.Query(ctx, sql)
			}
			// set to all db
			for _, db := range executor.all {
				go func(db *DataSourceBrief) {
					if _, _, err := db.DB.Query(ctx, sql); err != nil {
						log.Error(err)
					}
				}(db)
			}
			return &mysql.Result{
				AffectedRows: 0,
				InsertId:     0,
			}, 0, nil
		}
	case *ast.BeginStmt:
		db = executor.masters.Next(proto.WithMaster(ctx)).(*DataSourceBrief)
		// TODO add metrics
		tx, result, err = db.DB.Begin(ctx)
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
		if result, err = tx.Commit(ctx); err != nil {
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
		if result, err = tx.Rollback(ctx); err != nil {
			return nil, 0, err
		}
		return result, 0, err
	case *ast.InsertStmt, *ast.DeleteStmt, *ast.UpdateStmt:
		txi, ok := executor.localTransactionMap.Load(connectionID)
		if ok {
			tx = txi.(proto.Tx)
			return tx.Query(ctx, sql)
		}
		db = executor.masters.Next(proto.WithMaster(ctx)).(*DataSourceBrief)
		return db.DB.Query(proto.WithMaster(ctx), sql)
	default:
		txi, ok := executor.localTransactionMap.Load(connectionID)
		if ok {
			tx = txi.(proto.Tx)
			return tx.Query(ctx, sql)
		}
		db = executor.reads.Next(proto.WithSlave(ctx)).(*DataSourceBrief)
		return db.DB.Query(proto.WithSlave(ctx), sql)
	}
}

func (executor *ReadWriteSplittingExecutor) ExecutorComStmtExecute(ctx context.Context, stmt *proto.Stmt) (proto.Result, uint16, error) {
	connectionID := proto.ConnectionID(ctx)
	txi, ok := executor.localTransactionMap.Load(connectionID)
	if ok {
		tx := txi.(proto.Tx)
		return tx.ExecuteStmt(ctx, stmt)
	}
	switch stmt.StmtNode.(type) {
	case *ast.InsertStmt, *ast.DeleteStmt, *ast.UpdateStmt:
		db := executor.masters.Next(proto.WithMaster(ctx)).(*DataSourceBrief)
		return db.DB.ExecuteStmt(proto.WithMaster(ctx), stmt)
	case *ast.SelectStmt:
		db := executor.reads.Next(proto.WithSlave(ctx)).(*DataSourceBrief)
		return db.DB.ExecuteStmt(proto.WithSlave(ctx), stmt)
	default:
		return nil, 0, errors.Errorf("unsupported %t statement", stmt.StmtNode)
	}
}

func (executor *ReadWriteSplittingExecutor) ConnectionClose(ctx context.Context) {
	connectionID := proto.ConnectionID(ctx)
	txi, ok := executor.localTransactionMap.Load(connectionID)
	if !ok {
		return
	}
	tx := txi.(proto.Tx)
	if _, err := tx.Rollback(ctx); err != nil {
		log.Error(err)
	}
}

func (executor *ReadWriteSplittingExecutor) doPreFilter(ctx context.Context) error {
	for i := 0; i < len(executor.PreFilters); i++ {
		f := executor.PreFilters[i]
		err := f.PreHandle(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (executor *ReadWriteSplittingExecutor) doPostFilter(ctx context.Context, result proto.Result) error {
	for i := 0; i < len(executor.PostFilters); i++ {
		f := executor.PostFilters[i]
		err := f.PostHandle(ctx, result)
		if err != nil {
			return err
		}
	}
	return nil
}
