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

package plan

import (
	"context"
	"strings"

	"github.com/pkg/errors"

	"github.com/cectc/dbpack/pkg/constant"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/third_party/parser/ast"
	"github.com/cectc/dbpack/third_party/parser/format"
)

type DirectlyQueryPlan struct {
	Stmt     ast.Node
	Args     []interface{}
	Executor proto.DBGroupExecutor
}

func (p *DirectlyQueryPlan) Execute(ctx context.Context, hints ...*ast.TableOptimizerHint) (proto.Result, uint16, error) {
	var (
		sb  strings.Builder
		sql string
		err error
	)
	restoreCtx := format.NewRestoreCtx(constant.DBPackRestoreFormat, &sb)
	if err = p.Stmt.Restore(restoreCtx); err != nil {
		return nil, 0, errors.WithStack(err)
	}
	sql = sb.String()
	log.Debugf("directly query, db name: %s, sql: %s", p.Executor.GroupName(), sql)
	commandType := proto.CommandType(ctx)
	switch commandType {
	case constant.ComQuery:
		return p.Executor.Query(ctx, sql)
	case constant.ComStmtExecute:
		return p.Executor.PrepareQuery(ctx, sql, p.Args...)
	default:
		return nil, 0, nil
	}
}

type MultiDirectlyQueryPlan struct {
	Stmt  ast.Node
	Plans []*DirectlyQueryPlan
}

func (p *MultiDirectlyQueryPlan) Execute(ctx context.Context, hints ...*ast.TableOptimizerHint) (proto.Result, uint16, error) {
	var (
		result proto.Result
		warns  uint16
		err    error
	)
	for _, plan := range p.Plans {
		result, warns, err = plan.Execute(ctx, nil)
	}
	return result, warns, err
}
