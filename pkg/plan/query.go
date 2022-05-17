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
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/pkg/errors"

	"github.com/cectc/dbpack/pkg/constant"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/misc"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/third_party/parser/ast"
	"github.com/cectc/dbpack/third_party/parser/format"
	driver "github.com/cectc/dbpack/third_party/types/parser_driver"
)

type QueryOnSingleDBPlan struct {
	Database string
	Tables   []string
	Stmt     *ast.SelectStmt
	Limit    *Limit
	Args     []interface{}
	Executor proto.DBGroupExecutor
}

type Limit struct {
	ArgsWithoutLimit []interface{}
	Offset           int64
	Count            int64
}

func (p *QueryOnSingleDBPlan) Execute(ctx context.Context) (proto.Result, uint16, error) {
	var (
		sb   strings.Builder
		args []interface{}
		err  error
	)
	p.castLimit(p.Stmt.Limit)
	if err = p.generate(&sb, &args); err != nil {
		return nil, 0, errors.Wrap(err, "failed to generate sql")
	}
	sql := sb.String()
	log.Debugf("query on single db, db name: %s, sql: %s", p.Database, sql)
	commandType := proto.CommandType(ctx)
	switch commandType {
	case constant.ComQuery:
		result, warnings, err := p.Executor.Query(ctx, sql)
		if err != nil {
			return nil, 0, errors.WithStack(err)
		}
		return result, warnings, nil
	case constant.ComStmtExecute:
		result, warnings, err := p.Executor.PrepareQuery(ctx, sql, args...)
		if err != nil {
			return nil, 0, errors.WithStack(err)
		}
		return result, warnings, nil
	}
	return nil, 0, nil
}

func (p *QueryOnSingleDBPlan) generate(sb *strings.Builder, args *[]interface{}) (err error) {
	switch len(p.Tables) {
	case 0:
		err = generateSelect("", p.Stmt, sb, p.Limit)
		p.appendArgs(args)
	case 1:
		// single shard table
		err = generateSelect(p.Tables[0], p.Stmt, sb, p.Limit)
		p.appendArgs(args)
	default:
		if p.Stmt.OrderBy != nil {
			sb.WriteString("SELECT * FROM (")
		}
		sb.WriteByte('(')
		if err = generateSelect(p.Tables[0], p.Stmt, sb, p.Limit); err != nil {
			return
		}
		sb.WriteByte(')')
		p.appendArgs(args)

		for i := 1; i < len(p.Tables); i++ {
			sb.WriteString(" UNION ALL ")

			sb.WriteByte('(')
			if err = generateSelect(p.Tables[i], p.Stmt, sb, p.Limit); err != nil {
				return
			}
			sb.WriteByte(')')
			p.appendArgs(args)
		}
		if p.Stmt.OrderBy != nil {
			sb.WriteString(") t ")
			restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, sb)
			if err := p.Stmt.OrderBy.Restore(restoreCtx); err != nil {
				return errors.WithStack(err)
			}
		}
	}
	return
}

func (p *QueryOnSingleDBPlan) castLimit(limit *ast.Limit) {
	if p.Stmt.Limit != nil {
		var (
			offset, count  int64
			limitArgsCount = 0
			err            error
		)
		if of, ok := p.Stmt.Limit.Offset.(*driver.ValueExpr); ok {
			offset = of.GetInt64()
		}
		if of, ok := p.Stmt.Limit.Offset.(*driver.ParamMarkerExpr); ok {
			offsetString := fmt.Sprintf("%v", p.Args[of.Order])
			offset, err = strconv.ParseInt(offsetString, 10, 64)
			if err != nil {
				log.Fatal(err)
			}
			limitArgsCount += 1
		}
		if ct, ok := p.Stmt.Limit.Count.(*driver.ValueExpr); ok {
			count = ct.GetInt64()
		}
		if ct, ok := p.Stmt.Limit.Count.(*driver.ParamMarkerExpr); ok {
			countString := fmt.Sprintf("%v", p.Args[ct.Order])
			count, err = strconv.ParseInt(countString, 10, 64)
			if err != nil {
				log.Fatal(err)
			}
			limitArgsCount += 1
		}
		p.Limit = &Limit{
			ArgsWithoutLimit: p.Args[:len(p.Args)-limitArgsCount],
			Offset:           offset,
			Count:            count,
		}
	}
}

func (p *QueryOnSingleDBPlan) appendArgs(args *[]interface{}) {
	if p.Limit != nil {
		*args = append(*args, p.Limit.ArgsWithoutLimit...)
	} else {
		*args = append(*args, p.Args...)
	}
}

type QueryOnMultiDBPlan struct {
	Stmt  *ast.SelectStmt
	Plans []*QueryOnSingleDBPlan
}

func (p *QueryOnMultiDBPlan) Execute(ctx context.Context) (proto.Result, uint16, error) {
	resultChan := make(chan *ResultWithErr, len(p.Plans))
	var wg sync.WaitGroup
	wg.Add(len(p.Plans))
	for _, plan := range p.Plans {
		go func(plan *QueryOnSingleDBPlan) {
			result, warn, err := plan.Execute(ctx)
			rlt := &ResultWithErr{
				Database: plan.Database,
				Result:   result,
				Warning:  warn,
				Error:    err,
			}
			resultChan <- rlt
			wg.Done()
		}(plan)
	}
	wg.Wait()
	close(resultChan)

	resultList := make([]*ResultWithErr, 0, len(p.Plans))
	for rlt := range resultChan {
		if rlt.Error != nil {
			return rlt.Result, rlt.Warning, rlt.Error
		}
		resultList = append(resultList, rlt)
	}
	sort.Sort(ResultWithErrs(resultList))
	result, warn := mergeResult(ctx, resultList, p.Stmt.OrderBy, p.Plans[0].Limit)
	return result, warn, nil
}

func generateSelect(table string, stmt *ast.SelectStmt, sb *strings.Builder, limit *Limit) error {
	sb.WriteString("SELECT ")

	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, sb)
	if err := stmt.Fields.Restore(restoreCtx); err != nil {
		return errors.WithStack(err)
	}

	if len(table) > 0 {
		sb.WriteString(" FROM ")
		handleFrom(sb, table, stmt.From)
	} else {
		if err := stmt.From.Restore(restoreCtx); err != nil {
			return errors.WithStack(err)
		}
	}

	if stmt.Where != nil {
		sb.WriteString(" WHERE ")
		if err := stmt.Where.Restore(restoreCtx); err != nil {
			return errors.WithStack(err)
		}
	}

	if stmt.GroupBy != nil {
		sb.WriteByte(' ')
		if err := stmt.GroupBy.Restore(restoreCtx); err != nil {
			return errors.WithStack(err)
		}
	}

	if stmt.Having != nil {
		sb.WriteByte(' ')
		if err := stmt.Having.Restore(restoreCtx); err != nil {
			return errors.WithStack(err)
		}
	}

	if stmt.OrderBy != nil {
		sb.WriteByte(' ')
		if err := stmt.OrderBy.Restore(restoreCtx); err != nil {
			return errors.WithStack(err)
		}
	}

	if limit != nil {
		sb.WriteByte(' ')
		limitCount := limit.Offset + limit.Count
		sb.WriteString(fmt.Sprintf("limit %d", limitCount))
	}

	return nil
}

func handleFrom(sb *strings.Builder, table string, from *ast.TableRefsClause) {
	if from.TableRefs.Right != nil {
		log.Fatal("unsupported table join")
	}
	first := from.TableRefs.Left.(*ast.TableSource)

	misc.Wrap(sb, '`', table)

	if first.AsName.String() != "" {
		sb.WriteString(" AS ")
		misc.Wrap(sb, '`', first.AsName.String())
	}
}
