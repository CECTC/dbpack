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

	"github.com/pkg/errors"

	"github.com/cectc/dbpack/pkg/cond"
	"github.com/cectc/dbpack/pkg/plan"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/pkg/topo"
	"github.com/cectc/dbpack/third_party/parser/ast"
)

func (o Optimizer) optimizeUpdate(ctx context.Context, stmt *ast.UpdateStmt, args []interface{}) (proto.Plan, error) {
	var (
		alg      cond.ShardingAlgorithm
		topology *topo.Topology
		exists   bool
	)
	tableName := stmt.TableRefs.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName).Name.String()
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

			return &plan.UpdateOnSingleDBPlan{
				Database: k,
				Tables:   v,
				Stmt:     stmt,
				Args:     args,
				Executor: executor,
			}, nil
		}
	}

	plans := make([]*plan.UpdateOnSingleDBPlan, 0, len(shards))

	for k, v := range shardMap {
		executor, exists := o.dbGroupExecutors[k]
		if !exists {
			return nil, errors.Errorf("db group %s should not be nil", k)
		}

		plans = append(plans, &plan.UpdateOnSingleDBPlan{
			Database: k,
			Tables:   v,
			Stmt:     stmt,
			Args:     args,
			Executor: executor,
		})
	}

	multiPlan := &plan.UpdateOnMultiDBPlan{
		Stmt:  stmt,
		Plans: plans,
	}
	return multiPlan, nil
}
