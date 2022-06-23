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

	"github.com/pkg/errors"

	"github.com/cectc/dbpack/pkg/cond"
	"github.com/cectc/dbpack/pkg/config"
	"github.com/cectc/dbpack/pkg/filter"
	"github.com/cectc/dbpack/pkg/lb"
	"github.com/cectc/dbpack/pkg/log"
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

	all                 []*DataSourceBrief
	optimizer           proto.Optimizer
	localTransactionMap map[uint32]proto.Tx
}

func NewShardingExecutor(conf *config.Executor) (proto.Executor, error) {
	var (
		err            error
		content        []byte
		shardingConfig *config.ShardingConfig
		all            []*DataSourceBrief
		executors      map[string]proto.DBGroupExecutor
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
	all, executors, err = convertDBGroupConfigsToExecutors(shardingConfig.DBGroups)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	algorithms, topologies, err = convertLogicTableConfigsToShardingAlgorithms(shardingConfig.LogicTables)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	executor := &ShardingExecutor{
		PreFilters:          make([]proto.DBPreFilter, 0),
		PostFilters:         make([]proto.DBPostFilter, 0),
		all:                 all,
		optimizer:           optimize.NewOptimizer(executors, algorithms, topologies),
		localTransactionMap: make(map[uint32]proto.Tx, 0),
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

func convertDBGroupConfigsToExecutors(dbGroups []*config.DataSourceRefGroup) ([]*DataSourceBrief, map[string]proto.DBGroupExecutor, error) {
	var (
		all    []*DataSourceBrief
		result = make(map[string]proto.DBGroupExecutor, 0)
	)
	for _, dbGroup := range dbGroups {
		allDBs, masters, reads, err := groupDataSourceRefs(dbGroup.DataSources)
		if err != nil {
			return nil, nil, err
		}

		masterLB, err := lb.New(dbGroup.LBAlgorithm)
		if err != nil {
			return nil, nil, err
		}
		readLB, err := lb.New(dbGroup.LBAlgorithm)
		if err != nil {
			return nil, nil, err
		}
		for _, master := range masters {
			masterLB.Add(master)
		}
		for _, read := range reads {
			readLB.Add(read)
		}

		exec := &DBGroupExecutor{
			dbGroupName: dbGroup.Name,
			masters:     masterLB,
			reads:       readLB,
		}
		result[exec.dbGroupName] = exec
		all = append(all, allDBs...)
	}
	return all, result, nil
}

func convertLogicTableConfigsToShardingAlgorithms(logicTables []*config.LogicTable) (
	map[string]cond.ShardingAlgorithm,
	map[string]*topo.Topology,
	error) {
	var (
		algs  = make(map[string]cond.ShardingAlgorithm, 0)
		topos = make(map[string]*topo.Topology, 0)
	)
	for _, table := range logicTables {
		topology, err := topo.ParseTopology(table.DBName, table.TableName, table.Topology)
		if err != nil {
			return nil, nil, err
		}
		topos[table.TableName] = topology
		alg := cond.NewNumberMod(table.ShardingRule.Column, table.AllowFullScan, topology)
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
	return config.RWS
}

func (executor *ShardingExecutor) ProcessDistributedTransaction() bool {
	return false
}

func (executor *ShardingExecutor) InLocalTransaction(ctx context.Context) bool {
	connectionID := proto.ConnectionID(ctx)
	_, ok := executor.localTransactionMap[connectionID]
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

func (executor *ShardingExecutor) ExecutorComQuery(ctx context.Context, sql string) (proto.Result, uint16, error) {
	var (
		plan proto.Plan
		err  error
	)
	newCtx, span := tracing.GetTraceSpan(ctx, "sharding_execute_com_query")
	defer span.End()

	log.Debugf("query: %s", sql)
	queryStmt := proto.QueryStmt(newCtx)
	if queryStmt == nil {
		return nil, 0, errors.New("query stmt should not be nil")
	}
	if _, ok := queryStmt.(*ast.SetStmt); ok {
		for _, db := range executor.all {
			go func(db *DataSourceBrief) {
				if _, _, err := db.DB.Query(newCtx, sql); err != nil {
					log.Error(err)
				}
			}(db)
		}

		return &mysql.Result{
			AffectedRows: 0,
			InsertId:     0,
		}, 0, nil
	}
	if selectStmt, ok := queryStmt.(*ast.SelectStmt); ok {
		if selectStmt.Fields != nil && len(selectStmt.Fields.Fields) > 0 {
			if _, ok := selectStmt.Fields.Fields[0].Expr.(*ast.VariableExpr); ok {
				return executor.all[0].DB.Query(newCtx, sql)
			}
		}
	}

	plan, err = executor.optimizer.Optimize(newCtx, queryStmt)
	if err != nil {
		return nil, 0, err
	}

	return plan.Execute(newCtx)
}

func (executor *ShardingExecutor) ExecutorComStmtExecute(ctx context.Context, stmt *proto.Stmt) (proto.Result, uint16, error) {
	var (
		args []interface{}
		plan proto.Plan
		err  error
	)

	for i := 0; i < len(stmt.BindVars); i++ {
		parameterID := fmt.Sprintf("v%d", i+1)
		args = append(args, stmt.BindVars[parameterID])
	}
	plan, err = executor.optimizer.Optimize(ctx, stmt.StmtNode, args...)
	if err != nil {
		return nil, 0, err
	}

	return plan.Execute(ctx)
}

func (executor *ShardingExecutor) ConnectionClose(ctx context.Context) {
	connectionID := proto.ConnectionID(ctx)
	tx, ok := executor.localTransactionMap[connectionID]
	if !ok {
		return
	}
	// TODO add metrics
	if _, err := tx.Rollback(ctx); err != nil {
		log.Error(err)
	}
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

func (executor *ShardingExecutor) doPostFilter(ctx context.Context, result proto.Result) error {
	for i := 0; i < len(executor.PostFilters); i++ {
		f := executor.PostFilters[i]
		err := f.PostHandle(ctx, result)
		if err != nil {
			return err
		}
	}
	return nil
}
