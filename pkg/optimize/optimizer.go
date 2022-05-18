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

package optimize

import (
	"context"
	"sort"

	"github.com/pkg/errors"

	"github.com/cectc/dbpack/pkg/cond"
	"github.com/cectc/dbpack/pkg/dt/schema"
	"github.com/cectc/dbpack/pkg/meta"
	"github.com/cectc/dbpack/pkg/plan"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/pkg/resource"
	"github.com/cectc/dbpack/pkg/topo"
	"github.com/cectc/dbpack/third_party/parser/ast"
	"github.com/cectc/dbpack/third_party/parser/opcode"
)

type Optimizer struct {
	// dbName -> DBGroupExecutor
	dbGroupExecutors map[string]proto.DBGroupExecutor
	// tableName -> ShardingAlgorithm
	algorithms map[string]cond.ShardingAlgorithm
	// tableName -> topology
	topologies map[string]*topo.Topology
}

func NewOptimizer(dbGroupExecutors map[string]proto.DBGroupExecutor,
	algorithms map[string]cond.ShardingAlgorithm,
	topologies map[string]*topo.Topology) proto.Optimizer {
	return &Optimizer{
		dbGroupExecutors: dbGroupExecutors,
		algorithms:       algorithms,
		topologies:       topologies,
	}
}

func (o Optimizer) Optimize(ctx context.Context, stmt ast.StmtNode, args ...interface{}) (proto.Plan, error) {
	switch t := stmt.(type) {
	case *ast.SelectStmt:
		return o.optimizeSelect(ctx, t, args)
	case *ast.InsertStmt:
		return o.optimizeInsert(ctx, t, args)
	case *ast.DeleteStmt:
	case *ast.UpdateStmt:
	}

	//TODO implement all statements
	panic("implement me")
}

func (o Optimizer) optimizeSelect(ctx context.Context, stmt *ast.SelectStmt, args []interface{}) (proto.Plan, error) {
	var (
		alg      cond.ShardingAlgorithm
		topology *topo.Topology
		exists   bool
	)
	tableName := stmt.From.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName).Name.String()

	if alg, exists = o.algorithms[tableName]; !exists {
		return nil, errors.New("sharding algorithm should not be nil")
	}
	if topology, exists = o.topologies[tableName]; !exists {
		return nil, errors.New("topology should not be nil")
	}

	condition, err := cond.ParseCondition(stmt.Where, args...)
	if err != nil {
		return nil, errors.Wrap(err, "parse condition failed")
	}
	cd := condition.(cond.ConditionShard)
	shards, err := cd.Shard(alg)
	if err != nil {
		return nil, errors.Wrap(err, "compute shards failed")
	}

	fullScan, shardMap := shards.ParseTopology(topology)
	if fullScan && !alg.AllowFullScan() {
		return nil, errors.New("full scan not allowed")
	}

	if len(shardMap) == 1 {
		for k, v := range shardMap {
			executor, exists := o.dbGroupExecutors[k]
			if !exists {
				return nil, errors.Errorf("db group %s should not be nil", k)
			}

			return &plan.QueryOnSingleDBPlan{
				Database: k,
				Tables:   v,
				Stmt:     stmt,
				Args:     args,
				Executor: executor,
			}, nil
		}
	}

	plans := make([]*plan.QueryOnSingleDBPlan, 0, len(shards))

	keys := make([]string, 0)
	for k := range shardMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		executor, exists := o.dbGroupExecutors[k]
		if !exists {
			return nil, errors.Errorf("db group %s should not be nil", k)
		}

		plans = append(plans, &plan.QueryOnSingleDBPlan{
			Database: k,
			Tables:   shardMap[k],
			Stmt:     stmt,
			Args:     args,
			Executor: executor,
		})
	}

	unionPlan := &plan.QueryOnMultiDBPlan{
		Stmt:  stmt,
		Plans: plans,
	}
	return unionPlan, nil
}

func (o Optimizer) optimizeInsert(ctx context.Context, stmt *ast.InsertStmt, args []interface{}) (proto.Plan, error) {
	var (
		alg       cond.ShardingAlgorithm
		topology  *topo.Topology
		tableMeta schema.TableMeta
		columns   []string
		exists    bool
		err       error
	)
	tableName := stmt.Table.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName).Name.String()
	for _, column := range stmt.Columns {
		columns = append(columns, column.Name.String())
	}

	if alg, exists = o.algorithms[tableName]; !exists {
		return nil, errors.New("sharding algorithm should not be nil")
	}
	if topology, exists = o.topologies[tableName]; !exists {
		return nil, errors.New("topology should not be nil")
	}

	for db, tables := range topology.DBs {
		sqlDB := resource.GetDBManager().GetDB(db)
		tableMeta, err = meta.GetTableMetaCache().GetTableMeta(ctx, sqlDB, tables[0])
		if err != nil {
			continue
		} else {
			break
		}
	}
	pk := tableMeta.GetPKName()
	index := findPkIndex(stmt, pk)
	// todo if index == 0, automatic generate a pk
	pkValue := args[index]

	cd := &cond.KeyCondition{
		Key:   pk,
		Op:    opcode.EQ,
		Value: pkValue,
	}
	shards, err := cd.Shard(alg)
	if err != nil {
		return nil, errors.Wrap(err, "compute shards failed")
	}
	fullScan, shardMap := shards.ParseTopology(topology)
	if fullScan && !alg.AllowFullScan() {
		return nil, errors.New("full scan not allowed")
	}

	if len(shardMap) == 1 {
		for k, v := range shardMap {
			executor, exists := o.dbGroupExecutors[k]
			if !exists {
				return nil, errors.Errorf("db group %s should not be nil", k)
			}

			return &plan.InsertPlan{
				Database: k,
				Table:    v[0],
				Columns:  columns,
				Stmt:     stmt,
				Args:     args,
				Executor: executor,
			}, nil
		}
	}
	return nil, errors.New("should never happen!")
}

func findPkIndex(stmt *ast.InsertStmt, pk string) int {
	if stmt.Columns != nil {
		for i, column := range stmt.Columns {
			if column.Name.String() == pk {
				return i
			}
		}
	}
	return 0
}
