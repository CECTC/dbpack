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
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/pkg/topo"
	"github.com/cectc/dbpack/third_party/parser/ast"
)

type Optimizer struct {
	appid        string
	globalTables map[string]bool
	executors    []proto.DBGroupExecutor
	// dbName -> DBGroupExecutor
	dbGroupExecutors map[string]proto.DBGroupExecutor
	// tableName -> ShardingAlgorithm
	algorithms map[string]cond.ShardingAlgorithm
	// tableName -> topology
	topologies map[string]*topo.Topology
}

func NewOptimizer(appid string,
	globalTables map[string]bool,
	executors []proto.DBGroupExecutor,
	dbGroupExecutors map[string]proto.DBGroupExecutor,
	algorithms map[string]cond.ShardingAlgorithm,
	topologies map[string]*topo.Topology) proto.Optimizer {
	return &Optimizer{
		appid:            appid,
		globalTables:     globalTables,
		executors:        executors,
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
		return o.optimizeDelete(ctx, t, args)
	case *ast.UpdateStmt:
		return o.optimizeUpdate(ctx, t, args)
	case *ast.ShowStmt:
		switch t.Tp {
		case ast.ShowTableStatus:
			return o.optimizeShowTableStatus(ctx, t, args)
		case ast.ShowTables:
			return o.optimizeShowTables(ctx, t, args)
		}
	}
	sqlText := proto.SqlText(ctx)
	return nil, errors.Errorf("unsupported statement type, sql: %s", sqlText)
}
