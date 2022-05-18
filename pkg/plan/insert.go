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

	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/third_party/parser/ast"
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
		return nil, 0, errors.Wrap(err, "failed to generate sql")
	}
	sql := sb.String()
	log.Debugf("insert, db name: %s, sql: %s", p.Database, sql)
	result, warnings, err := p.Executor.PrepareQuery(ctx, sql, p.Args...)
	if err != nil {
		return nil, 0, errors.WithStack(err)
	}
	return result, warnings, nil
}

func (p *InsertPlan) generate(sb *strings.Builder) (err error) {
	sb.WriteString("INSERT INTO ")
	sb.WriteString(p.Table)
	sb.WriteByte('(')
	columnLen := len(p.Columns)
	for i, column := range p.Columns {
		sb.WriteString(column)
		if i != columnLen-1 {
			sb.WriteByte(',')
		}
	}
	sb.WriteByte(')')
	sb.WriteString(" VALUES (")
	for i, _ := range p.Columns {
		sb.WriteByte('?')
		if i != columnLen-1 {
			sb.WriteByte(',')
		}
	}
	sb.WriteByte(')')
	return nil
}
