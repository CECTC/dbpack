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

type InsertPlan struct {
	Database string
	Table    string
	Columns  []string
	Stmt     *ast.InsertStmt
	Args     []interface{}
	Executor proto.DBGroupExecutor
}

func (p *InsertPlan) Execute(ctx context.Context) (proto.Result, uint16, error) {
	var (
		sb  strings.Builder
		err error
	)
	if err = p.generate(&sb); err != nil {
		return nil, 0, errors.WithStack(err)
	}
	sql := sb.String()
	log.Debugf("insert, db name: %s, sql: %s", p.Database, sql)
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

func (p *InsertPlan) generate(sb *strings.Builder) (err error) {
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, sb)

	ctx.WriteKeyWord("INSERT ")
	ctx.WriteKeyWord("INTO ")

	ctx.WritePlain(p.Table)

	ctx.WritePlain("(")
	columnLen := len(p.Columns)
	for i, column := range p.Columns {
		ctx.WritePlain(column)
		if i != columnLen-1 {
			ctx.WritePlain(",")
		}
	}
	ctx.WritePlain(")")

	if p.Stmt.Lists != nil {
		ctx.WriteKeyWord(" VALUES ")
		for i, row := range p.Stmt.Lists {
			if i != 0 {
				ctx.WritePlain(",")
			}
			ctx.WritePlain("(")
			for j, v := range row {
				if j != 0 {
					ctx.WritePlain(",")
				}
				if err := v.Restore(ctx); err != nil {
					return errors.Wrapf(err, "An error occurred while restoring InsertStmt.Lists[%d][%d]", i, j)
				}
			}
			ctx.WritePlain(")")
		}
	}
	return nil
}
