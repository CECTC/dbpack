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

func (s *QueryOnSingleDBPlan) Execute(ctx context.Context) (proto.Result, uint16, error) {
	var (
		sb   strings.Builder
		args []interface{}
		err  error
	)
	s.castLimit(s.Stmt.Limit)
	if err = s.generate(&sb, &args); err != nil {
		return nil, 0, errors.Wrap(err, "failed to generate sql")
	}
	sql := sb.String()
	log.Debugf("query on single db, db name: %s, sql: %s", s.Database, sql)
	commandType := proto.CommandType(ctx)
	switch commandType {
	case constant.ComQuery:
		result, warnings, err := s.Executor.Query(ctx, sql)
		if err != nil {
			return nil, 0, errors.WithStack(err)
		}
		return result, warnings, nil
	case constant.ComStmtExecute:
		result, warnings, err := s.Executor.PrepareQuery(ctx, sql, args...)
		if err != nil {
			return nil, 0, errors.WithStack(err)
		}
		return result, warnings, nil
	}
	return nil, 0, nil
}

func (s *QueryOnSingleDBPlan) generate(sb *strings.Builder, args *[]interface{}) (err error) {
	switch len(s.Tables) {
	case 0:
		err = generateSelect("", s.Stmt, sb, s.Limit)
		s.appendArgs(args)
	case 1:
		// single shard table
		err = generateSelect(s.Tables[0], s.Stmt, sb, s.Limit)
		s.appendArgs(args)
	default:
		if s.Stmt.OrderBy != nil {
			sb.WriteString("select * from (")
		}
		sb.WriteByte('(')
		if err = generateSelect(s.Tables[0], s.Stmt, sb, s.Limit); err != nil {
			return
		}
		sb.WriteByte(')')
		s.appendArgs(args)

		for i := 1; i < len(s.Tables); i++ {
			sb.WriteString(" UNION ALL ")

			sb.WriteByte('(')
			if err = generateSelect(s.Tables[i], s.Stmt, sb, s.Limit); err != nil {
				return
			}
			sb.WriteByte(')')
			s.appendArgs(args)
		}
		if s.Stmt.OrderBy != nil {
			sb.WriteString(") t ")
			restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, sb)
			if err := s.Stmt.OrderBy.Restore(restoreCtx); err != nil {
				return errors.WithStack(err)
			}
		}
	}
	return
}

func (s *QueryOnSingleDBPlan) castLimit(limit *ast.Limit) {
	if s.Stmt.Limit != nil {
		var (
			offset, count  int64
			limitArgsCount = 0
			err            error
		)
		if of, ok := s.Stmt.Limit.Offset.(*driver.ValueExpr); ok {
			offset = of.GetInt64()
		}
		if of, ok := s.Stmt.Limit.Offset.(*driver.ParamMarkerExpr); ok {
			offsetString := fmt.Sprintf("%v", s.Args[of.Order])
			offset, err = strconv.ParseInt(offsetString, 10, 64)
			if err != nil {
				log.Fatal(err)
			}
			limitArgsCount += 1
		}
		if ct, ok := s.Stmt.Limit.Count.(*driver.ValueExpr); ok {
			count = ct.GetInt64()
		}
		if ct, ok := s.Stmt.Limit.Count.(*driver.ParamMarkerExpr); ok {
			countString := fmt.Sprintf("%v", s.Args[ct.Order])
			count, err = strconv.ParseInt(countString, 10, 64)
			if err != nil {
				log.Fatal(err)
			}
			limitArgsCount += 1
		}
		s.Limit = &Limit{
			ArgsWithoutLimit: s.Args[:len(s.Args)-limitArgsCount],
			Offset:           offset,
			Count:            count,
		}
	}
}

func (s *QueryOnSingleDBPlan) appendArgs(args *[]interface{}) {
	if s.Limit != nil {
		*args = append(*args, s.Limit.ArgsWithoutLimit...)
	} else {
		*args = append(*args, s.Args...)
	}
}

type QueryOnMultiDBPlan struct {
	Stmt  *ast.SelectStmt
	Plans []*QueryOnSingleDBPlan
}

func (u QueryOnMultiDBPlan) Execute(ctx context.Context) (proto.Result, uint16, error) {
	resultChan := make(chan *ResultWithErr, len(u.Plans))
	var wg sync.WaitGroup
	wg.Add(len(u.Plans))
	for _, plan := range u.Plans {
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

	resultList := make([]*ResultWithErr, 0, len(u.Plans))
	for rlt := range resultChan {
		if rlt.Error != nil {
			return rlt.Result, rlt.Warning, rlt.Error
		}
		resultList = append(resultList, rlt)
	}
	sort.Sort(ResultWithErrs(resultList))
	result, warn := mergeResult(ctx, resultList, u.Stmt.OrderBy, u.Plans[0].Limit)
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
