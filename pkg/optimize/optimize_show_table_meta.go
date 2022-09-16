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
	"github.com/cectc/dbpack/third_party/parser/ast"
)

func (o Optimizer) optimizeShowTableMeta(ctx context.Context, stmt *ast.ShowStmt, args []interface{}) (proto.Plan, error) {
	if stmt.Tp != ast.ShowColumns && stmt.Tp != ast.ShowIndex {
		return nil, errors.New("statement must be show columns stmt or show index stmt")
	}

	executor := o.executors[0]
	tableName := stmt.Table.Name.O
	if topology, exists := o.topologies[tableName]; exists {
		stmt.Table.Name.O = topology.DBs[executor.GroupName()][0]
		return &plan.ShowTableMetaPlan{
			Stmt:     stmt,
			Args:     args,
			Executor: executor,
		}, nil
	}
	return &plan.ShowTableMetaPlan{
		Stmt:     stmt,
		Args:     args,
		Executor: executor,
	}, nil
}
