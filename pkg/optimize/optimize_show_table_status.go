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

	"github.com/cectc/dbpack/pkg/plan"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/pkg/topo"
	"github.com/cectc/dbpack/third_party/parser/ast"
	driver "github.com/cectc/dbpack/third_party/types/parser_driver"
)

func (o Optimizer) optimizeShowTableStatus(ctx context.Context, stmt *ast.ShowStmt, args []interface{}) (proto.Plan, error) {
	var (
		topology *topo.Topology
		exists   bool
	)

	if stmt.Tp != ast.ShowTableStatus {
		return nil, errors.New("statement must be show table status stmt")
	}

	pattern := stmt.Pattern.Pattern.(*driver.ValueExpr)
	tableName := pattern.GetDatumString()
	if topology, exists = o.topologies[tableName]; !exists {
		return &plan.ShowTableStatusPlan{
			Stmt:     stmt,
			Args:     args,
			Executor: o.executors[0],
		}, nil
	} else {
		table := topology.DBs[o.executors[0].GroupName()][0]
		pattern.SetValue(table)
		return &plan.ShowTableStatusPlan{
			Stmt:     stmt,
			Args:     args,
			Executor: o.executors[0],
		}, nil
	}
}
