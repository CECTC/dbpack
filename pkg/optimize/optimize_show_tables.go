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

func (o Optimizer) optimizeShowTables(ctx context.Context, stmt *ast.ShowStmt, args []interface{}) (proto.Plan, error) {
	var logicTables []string
	if stmt.Tp != ast.ShowTables {
		return nil, errors.New("statement must be show tables stmt")
	}

	for logicTable := range o.topologies {
		logicTables = append(logicTables, logicTable)
	}

	return &plan.ShowTablesPlan{
		Stmt:        stmt,
		Args:        args,
		Executor:    o.executors[0],
		LogicTables: logicTables,
	}, nil
}
