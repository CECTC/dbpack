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
	"fmt"
	"strings"
	"sync"

	"github.com/pkg/errors"

	"github.com/cectc/dbpack/pkg/cond"
	"github.com/cectc/dbpack/pkg/config"
	"github.com/cectc/dbpack/pkg/constant"
	"github.com/cectc/dbpack/pkg/filter"
	"github.com/cectc/dbpack/pkg/group"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/misc/uuid"
	"github.com/cectc/dbpack/pkg/mysql"
	"github.com/cectc/dbpack/pkg/optimize"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/pkg/topo"
	"github.com/cectc/dbpack/pkg/tracing"
	"github.com/cectc/dbpack/third_party/parser/ast"
)

type ShardingExecutor struct {
	PreFilters  []proto.DBPreFilter
	PostFilters []proto.DBPostFilter

	config    *config.ShardingConfig
	executors []proto.DBGroupExecutor
	optimizer proto.Optimizer
	// map[uint32]proto.DBGroupTx
	localTransactionMap *sync.Map
}

func NewShardingExecutor(conf *config.Executor) (proto.Executor, error) {
	var (
		err            error
		content        []byte
		shardingConfig *config.ShardingConfig
		globalTables   = make(map[string]bool)
		executorSlice  []proto.DBGroupExecutor
		executorMap    = make(map[string]proto.DBGroupExecutor)
		algorithms     map[string]cond.ShardingAlgorithm
		topologies     map[string]*topo.Topology
	)

	if content, err = json.Marshal(conf.Config); err != nil {
		return nil, errors.Wrap(err, "marshal sharding executor config failed.")
	}

	if err = json.Unmarshal(content, &shardingConfig); err != nil {
		log.Errorf("unmarshal read sharding executor config failed, %s", err)
		return nil, err
	}

	for _, globalTable := range shardingConfig.GlobalTables {
		globalTables[strings.ToLower(globalTable)] = true
	}

	for _, groupConfig := range shardingConfig.DBGroups {
		dbGroup, err := group.NewDBGroup(conf.AppID, groupConfig.Name, groupConfig.LBAlgorithm, groupConfig.DataSources)
		if err != nil {
			return nil, err
		}
		executorSlice = append(executorSlice, dbGroup)
		executorMap[dbGroup.GroupName()] = dbGroup
	}

	algorithms, topologies, err = convertShardingAlgorithmsAndTopologies(shardingConfig.LogicTables)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	executor := &ShardingExecutor{
		PreFilters:  make([]proto.DBPreFilter, 0),
		PostFilters: make([]proto.DBPostFilter, 0),
		config:      shardingConfig,
		executors:   executorSlice,
		optimizer: optimize.NewOptimizer(conf.AppID,
			globalTables, executorSlice, executorMap, algorithms, topologies),
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

func convertShardingAlgorithmsAndTopologies(logicTables []*config.LogicTable) (
	map[string]cond.ShardingAlgorithm,
	map[string]*topo.Topology,
	error) {
	var (
		algs      = make(map[string]cond.ShardingAlgorithm, 0)
		topos     = make(map[string]*topo.Topology, 0)
		generator uuid.Generator
	)
	for _, table := range logicTables {
		topology, err := topo.ParseTopology(table.DBName, table.TableName, table.Topology)
		if err != nil {
			return nil, nil, err
		}
		topos[table.TableName] = topology

		if table.ShardingKeyGenerator.Type == "segment" {
			generator, err = uuid.NewSegmentWorker(table.ShardingKeyGenerator.DSN, 1000, table.TableName)
			if err != nil {
				log.Errorf("segment dsn %s, table name %s, err %s", table.ShardingKeyGenerator.DSN, table.TableName, err)
				return nil, nil, err
			}
		} else {
			generator, err = uuid.NewWorker(table.ShardingKeyGenerator.Worker)
			if err != nil {
				log.Errorf("init snowflake id generator, err %s", err)
				return nil, nil, err
			}
		}
		alg, err := cond.NewShardingAlgorithm(table.ShardingRule.ShardingAlgorithm,
			table.ShardingRule.Column, table.AllowFullScan, topology, table.ShardingRule.Config, generator)
		if err != nil {
			return nil, nil, err
		}
		algs[table.TableName] = alg
	}
	return algs, topos, nil
}

func (executor *ShardingExecutor) GetPreFilters() []proto.DBPreFilter {
	return executor.PreFilters
}

func (executor *ShardingExecutor) GetPostFilters() []proto.DBPostFilter {
	return executor.PostFilters
}

func (executor *ShardingExecutor) ExecuteMode() config.ExecuteMode {
	return config.SHD
}

func (executor *ShardingExecutor) ProcessDistributedTransaction() bool {
	return false
}

func (executor *ShardingExecutor) InLocalTransaction(ctx context.Context) bool {
	connectionID := proto.ConnectionID(ctx)
	_, ok := executor.localTransactionMap.Load(connectionID)
	return ok
}

func (executor *ShardingExecutor) InGlobalTransaction(ctx context.Context) bool {
	return false
}

func (executor *ShardingExecutor) ExecuteUseDB(ctx context.Context, db string) error {
	return errors.New("unimplemented COM_INIT_DB in read write splitting mode")
}

// ExecuteFieldList
// https://dev.mysql.com/doc/internals/en/com-field-list.html
// As of MySQL 5.7.11, COM_FIELD_LIST is deprecated and will be removed in a future version of MySQL.
// Instead, use mysql_query() to execute a SHOW COLUMNS statement.
func (executor *ShardingExecutor) ExecuteFieldList(ctx context.Context, table, wildcard string) ([]proto.Field, error) {
	return nil, errors.New("unimplemented COM_FIELD_LIST in read write splitting mode")
}

func (executor *ShardingExecutor) ExecutorComQuery(ctx context.Context, sql string) (result proto.Result, warn uint16, err error) {
	proto.WithVariable(ctx, constant.TransactionTimeout, executor.config.TransactionTimeout)
	spanCtx, span := tracing.GetTraceSpan(ctx, tracing.SHDComQuery)
	defer span.End()

	if err = executor.doPreFilter(spanCtx); err != nil {
		return nil, 0, err
	}
	defer func() {
		if err == nil {
			result, err = decodeResult(result)
		}
		err = executor.doPostFilter(spanCtx, result, err)
		if err != nil {
			span.RecordError(err)
		}
	}()

	var plan proto.Plan

	log.Debugf("query: %s", sql)
	connectionID := proto.ConnectionID(spanCtx)
	queryStmt := proto.QueryStmt(spanCtx)
	if queryStmt == nil {
		return nil, 0, errors.New("query stmt should not be nil")
	}

	switch stmt := queryStmt.(type) {
	case *ast.SetStmt:
		if shouldStartTransaction(stmt) {
			tx := group.NewComplexTx(executor.optimizer)
			executor.localTransactionMap.Store(connectionID, tx)
		} else {
			for _, db := range executor.executors {
				go func(dbGroup proto.DBGroupExecutor) {
					if _, _, err := dbGroup.QueryAll(spanCtx, sql); err != nil {
						log.Error(err)
					}
				}(db)
			}
		}
		return &mysql.Result{
			AffectedRows: 0,
			InsertId:     0,
		}, 0, nil
	case *ast.ShowStmt:
		switch stmt.Tp {
		case ast.ShowEngines, ast.ShowDatabases, ast.ShowCreateDatabase:
			return executor.executors[0].Query(spanCtx, sql)
		}
		plan, err = executor.optimizer.Optimize(spanCtx, queryStmt)
		if err != nil {
			return nil, 0, err
		}
		return plan.Execute(spanCtx)
	case *ast.BeginStmt:
		tx := group.NewComplexTx(executor.optimizer)
		executor.localTransactionMap.Store(connectionID, tx)
		return &mysql.Result{
			AffectedRows: 0,
			InsertId:     0,
		}, 0, nil
	case *ast.CommitStmt:
		txi, ok := executor.localTransactionMap.Load(connectionID)
		if !ok {
			return nil, 0, errors.New("there is no transaction")
		}
		defer executor.localTransactionMap.Delete(connectionID)
		tx := txi.(proto.DBGroupTx)
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
		tx := txi.(proto.DBGroupTx)
		// TODO add metrics
		if result, err = tx.Rollback(spanCtx); err != nil {
			return nil, 0, err
		}
		return result, 0, err
	case *ast.SelectStmt:
		if stmt.Fields != nil && len(stmt.Fields.Fields) > 0 {
			if _, ok := stmt.Fields.Fields[0].Expr.(*ast.VariableExpr); ok {
				return executor.executors[0].Query(spanCtx, sql)
			}
		}
		txi, ok := executor.localTransactionMap.Load(connectionID)
		if ok {
			tx := txi.(proto.DBGroupTx)
			return tx.Query(spanCtx, sql)
		}
		plan, err = executor.optimizer.Optimize(spanCtx, queryStmt)
		if err != nil {
			return nil, 0, err
		}
		return plan.Execute(spanCtx)
	default:
		txi, ok := executor.localTransactionMap.Load(connectionID)
		if ok {
			tx := txi.(proto.DBGroupTx)
			return tx.Query(spanCtx, sql)
		}
		plan, err = executor.optimizer.Optimize(spanCtx, queryStmt)
		if err != nil {
			return nil, 0, err
		}
		return plan.Execute(spanCtx)
	}
}

func (executor *ShardingExecutor) ExecutorComStmtExecute(
	ctx context.Context, stmt *proto.Stmt) (result proto.Result, warns uint16, err error) {
	proto.WithVariable(ctx, constant.TransactionTimeout, executor.config.TransactionTimeout)
	spanCtx, span := tracing.GetTraceSpan(ctx, tracing.SHDComStmtExecute)
	defer span.End()

	if err = executor.doPreFilter(ctx); err != nil {
		return nil, 0, err
	}
	defer func() {
		if err == nil {
			result, err = decodeResult(result)
		}
		err = executor.doPostFilter(spanCtx, result, err)
		if err != nil {
			span.RecordError(err)
		}
	}()

	var (
		args []interface{}
		plan proto.Plan
	)

	connectionID := proto.ConnectionID(ctx)
	log.Debugf("connectionID: %d, prepare: %s", connectionID, stmt.SqlText)
	for i := 0; i < len(stmt.BindVars); i++ {
		parameterID := fmt.Sprintf("v%d", i+1)
		args = append(args, stmt.BindVars[parameterID])
	}

	txi, ok := executor.localTransactionMap.Load(connectionID)
	if ok {
		tx := txi.(proto.DBGroupTx)
		return tx.Execute(spanCtx, stmt.StmtNode, args...)
	}

	plan, err = executor.optimizer.Optimize(spanCtx, stmt.StmtNode, args...)
	if err != nil {
		return nil, 0, err
	}
	return plan.Execute(spanCtx)
}

func (executor *ShardingExecutor) ConnectionClose(ctx context.Context) {
	connectionID := proto.ConnectionID(ctx)
	txi, ok := executor.localTransactionMap.Load(connectionID)
	if !ok {
		return
	}
	// TODO add metrics
	tx := txi.(proto.DBGroupTx)
	if _, err := tx.Rollback(ctx); err != nil {
		log.Error(err)
	}
	executor.localTransactionMap.Delete(connectionID)
}

func (executor *ShardingExecutor) doPreFilter(ctx context.Context) error {
	for i := 0; i < len(executor.PreFilters); i++ {
		f := executor.PreFilters[i]
		err := f.PreHandle(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (executor *ShardingExecutor) doPostFilter(ctx context.Context, result proto.Result, err error) error {
	for i := 0; i < len(executor.PostFilters); i++ {
		f := executor.PostFilters[i]
		err := f.PostHandle(ctx, result, err)
		if err != nil {
			return err
		}
	}
	return err
}
