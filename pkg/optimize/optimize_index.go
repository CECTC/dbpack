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
	"fmt"
	"strings"

	"github.com/huandu/go-clone"
	"github.com/pkg/errors"

	"github.com/cectc/dbpack/pkg/plan"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/pkg/topo"
	"github.com/cectc/dbpack/third_party/parser/ast"
)

func (o Optimizer) optimizeCreateIndex(ctx context.Context, stmt *ast.CreateIndexStmt, args []interface{}) (proto.Plan, error) {
	var (
		topology *topo.Topology
		exists   bool
	)
	tableName := stmt.Table.Name.String()

	if o.globalTables[strings.ToLower(tableName)] {
		return &plan.DirectQueryPlan{
			Stmt:     stmt,
			Args:     args,
			Executor: o.executors[0],
		}, nil
	}

	if topology, exists = o.topologies[tableName]; !exists {
		return nil, errors.New(fmt.Sprintf("topology of %s should not be nil", tableName))
	}

	plans := &plan.MultiDirectlyQueryPlan{
		Stmt:  stmt,
		Plans: make([]*plan.DirectQueryPlan, 0),
	}
	for db, tables := range topology.DBs {
		for _, table := range tables {
			statement := clone.Clone(stmt)
			newStmt := statement.(*ast.CreateIndexStmt)
			newStmt.Table.Name.O = table
			directlyQueryPlan := &plan.DirectQueryPlan{
				Stmt:     newStmt,
				Args:     args,
				Executor: o.dbGroupExecutors[db],
			}
			plans.Plans = append(plans.Plans, directlyQueryPlan)
		}
	}
	return plans, nil
}

func (o Optimizer) optimizeDropIndex(ctx context.Context, stmt *ast.DropIndexStmt, args []interface{}) (proto.Plan, error) {
	var (
		topology *topo.Topology
		exists   bool
	)
	tableName := stmt.Table.Name.String()

	if o.globalTables[strings.ToLower(tableName)] {
		return &plan.DirectQueryPlan{
			Stmt:     stmt,
			Args:     args,
			Executor: o.executors[0],
		}, nil
	}

	if topology, exists = o.topologies[tableName]; !exists {
		return nil, errors.New(fmt.Sprintf("topology of %s should not be nil", tableName))
	}

	plans := &plan.MultiDirectlyQueryPlan{
		Stmt:  stmt,
		Plans: make([]*plan.DirectQueryPlan, 0),
	}
	for db, tables := range topology.DBs {
		for _, table := range tables {
			statement := clone.Clone(stmt)
			newStmt := statement.(*ast.DropIndexStmt)
			newStmt.Table.Name.O = table
			directlyQueryPlan := &plan.DirectQueryPlan{
				Stmt:     newStmt,
				Args:     args,
				Executor: o.dbGroupExecutors[db],
			}
			plans.Plans = append(plans.Plans, directlyQueryPlan)
		}
	}
	return plans, nil
}
