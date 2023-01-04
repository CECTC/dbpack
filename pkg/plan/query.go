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

	"github.com/mohae/deepcopy"
	"github.com/pkg/errors"

	"github.com/cectc/dbpack/pkg/cond"
	"github.com/cectc/dbpack/pkg/constant"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/pkg/visitor"
	"github.com/cectc/dbpack/third_party/parser/ast"
	"github.com/cectc/dbpack/third_party/parser/format"
	"github.com/cectc/dbpack/third_party/parser/model"
	driver "github.com/cectc/dbpack/third_party/types/parser_driver"
)

const FuncColumns = "FuncColumns"

type QueryOnSingleDBPlan struct {
	Database string
	Tables   []string
	PK       string
	Stmt     *ast.SelectStmt
	Limit    *Limit
	Args     []interface{}
	// tableName -> ShardingAlgorithm
	Algorithms   map[string]cond.ShardingAlgorithm
	GlobalTables map[string]bool
	Executor     proto.DBGroupExecutor
}

type Limit struct {
	ArgsWithoutLimit []interface{}
	Offset           int64
	Count            int64
}

func (p *QueryOnSingleDBPlan) Execute(ctx context.Context, hints ...*ast.TableOptimizerHint) (proto.Result, uint16, error) {
	var (
		sb   strings.Builder
		args []interface{}
		tx   proto.Tx
		err  error
	)
	p.castLimit()
	if err = p.generate(ctx, &sb, &args); err != nil {
		return nil, 0, errors.WithStack(err)
	}
	sql := sb.String()
	log.Debugf("query on single db, db name: %s, sql: %s", p.Database, sql)

	if complexTx := proto.ExtractDBGroupTx(ctx); complexTx != nil {
		tx, err = complexTx.Begin(ctx, p.Executor)
		if err != nil {
			return nil, 0, errors.WithStack(err)
		}
		commandType := proto.CommandType(ctx)
		switch commandType {
		case constant.ComQuery:
			return tx.Query(ctx, sql)
		case constant.ComStmtExecute:
			return tx.ExecuteSql(ctx, sql, args...)
		default:
			return nil, 0, nil
		}
	}

	commandType := proto.CommandType(ctx)
	switch commandType {
	case constant.ComQuery:
		return p.Executor.Query(ctx, sql)
	case constant.ComStmtExecute:
		return p.Executor.PrepareQuery(ctx, sql, args...)
	default:
		return nil, 0, nil
	}
}

func (p *QueryOnSingleDBPlan) generate(ctx context.Context, sb *strings.Builder, args *[]interface{}) (err error) {
	stmtVal := deepcopy.Copy(p.Stmt)
	stmt := stmtVal.(*ast.SelectStmt)
	switch len(p.Tables) {
	case 0:
		err = p.generateSelect("", stmt, sb, p.Limit)
		p.appendArgs(args)
	case 1:
		// single shard table
		err = p.generateSelect(p.Tables[0], stmt, sb, p.Limit)
		p.appendArgs(args)
	default:
		sb.WriteString("SELECT * FROM (")

		sb.WriteByte('(')
		if err = p.generateSelect(p.Tables[0], stmt, sb, p.Limit); err != nil {
			return
		}
		sb.WriteByte(')')
		p.appendArgs(args)

		for i := 1; i < len(p.Tables); i++ {
			stmtVal := deepcopy.Copy(p.Stmt)
			stmt := stmtVal.(*ast.SelectStmt)

			sb.WriteString(" UNION ALL ")
			sb.WriteByte('(')
			if err = p.generateSelect(p.Tables[i], stmt, sb, p.Limit); err != nil {
				return
			}
			sb.WriteByte(')')
			p.appendArgs(args)
		}
		sb.WriteString(") t ")
		if stmt.OrderBy != nil {
			orderByRewriteVisitor := &ColumnRewriteVisitor{
				rewriteTable: "t",
			}
			stmt.OrderBy.Accept(orderByRewriteVisitor)
			restoreCtx := format.NewRestoreCtx(constant.DBPackRestoreFormat, sb)
			if err := stmt.OrderBy.Restore(restoreCtx); err != nil {
				return errors.WithStack(err)
			}
		} else {
			var funcColumnList []*visitor.FuncColumn
			funcColumns := proto.Variable(ctx, FuncColumns)
			if funcColumns != nil {
				funcColumnList = funcColumns.([]*visitor.FuncColumn)
			}
			if len(funcColumnList) == 0 {
				sb.WriteString(fmt.Sprintf("ORDER BY `t`.`%s` ASC", p.PK))
			}
		}
	}
	return
}

func (p *QueryOnSingleDBPlan) castLimit() {
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

func (p *QueryOnMultiDBPlan) Execute(ctx context.Context, _ ...*ast.TableOptimizerHint) (proto.Result, uint16, error) {
	funcColumns := visitFuncColumn(p.Stmt)
	proto.WithVariable(ctx, FuncColumns, funcColumns)
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
	aggregateResult(ctx, result)
	return result, warn, nil
}

func (p *QueryOnSingleDBPlan) generateSelect(table string, stmt *ast.SelectStmt, sb *strings.Builder, limit *Limit) error {
	vi := &JoinVisitor{
		fieldList:    stmt.Fields,
		where:        stmt.Where,
		orderBy:      stmt.OrderBy,
		table:        table,
		algorithms:   p.Algorithms,
		globalTables: p.GlobalTables,
	}
	stmt.Accept(vi)

	ctx := format.NewRestoreCtx(constant.DBPackRestoreFormat, sb)
	ctx.WriteKeyWord(stmt.Kind.String())
	ctx.WritePlain(" ")

	if stmt.Distinct {
		ctx.WriteKeyWord("DISTINCT ")
	} else if stmt.SelectStmtOpts.ExplicitAll {
		ctx.WriteKeyWord("ALL ")
	}

	if stmt.Fields != nil {
		for i, field := range stmt.Fields.Fields {
			if i != 0 {
				ctx.WritePlain(",")
			}
			if err := field.Restore(ctx); err != nil {
				return errors.Wrapf(err, "An error occurred while restore SelectStmt.Fields[%d]", i)
			}
		}
	}

	if len(table) > 0 {
		ctx.WriteKeyWord(" FROM ")
		if err := p.handleFrom(sb, stmt.From); err != nil {
			return err
		}
	} else {
		if err := stmt.From.Restore(ctx); err != nil {
			return errors.WithStack(err)
		}
	}

	if stmt.Where != nil {
		ctx.WriteKeyWord(" WHERE ")
		if err := stmt.Where.Restore(ctx); err != nil {
			return errors.Wrapf(err, "An error occurred while restore SelectStmt.Where")
		}
	}

	if stmt.GroupBy != nil {
		ctx.WritePlain(" ")
		if err := stmt.GroupBy.Restore(ctx); err != nil {
			return errors.Wrapf(err, "An error occurred while restore SelectStmt.GroupBy")
		}
	}

	if stmt.Having != nil {
		ctx.WritePlain(" ")
		if err := stmt.Having.Restore(ctx); err != nil {
			return errors.Wrapf(err, "An error occurred while restore SelectStmt.Having")
		}
	}

	if stmt.OrderBy != nil {
		ctx.WritePlain(" ")
		if err := stmt.OrderBy.Restore(ctx); err != nil {
			return errors.Wrapf(err, "An error occurred while restore SelectStmt.OrderBy")
		}
	}

	if limit != nil {
		sb.WriteByte(' ')
		limitCount := limit.Offset + limit.Count
		sb.WriteString(fmt.Sprintf("LIMIT %d", limitCount))
	}

	return nil
}

func (p *QueryOnSingleDBPlan) handleFrom(sb *strings.Builder, from *ast.TableRefsClause) error {
	ctx := format.NewRestoreCtx(constant.DBPackRestoreFormat, sb)
	if _, ok := from.TableRefs.Left.(*ast.Join); ok {
		return errors.New("only support two tables join")
	}

	if err := from.Restore(ctx); err != nil {
		return err
	}
	return nil
}

type JoinVisitor struct {
	fieldList *ast.FieldList
	where     ast.ExprNode
	orderBy   *ast.OrderByClause

	table        string
	algorithms   map[string]cond.ShardingAlgorithm
	globalTables map[string]bool
}

func (s *JoinVisitor) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	return n, false
}

// Leave implement ast.Visitor
func (s *JoinVisitor) Leave(n ast.Node) (node ast.Node, ok bool) {
	from, ok := n.(*ast.TableRefsClause)
	if !ok {
		return n, true
	}
	first := from.TableRefs.Left.(*ast.TableSource)
	firstTable := first.Source.(*ast.TableName)
	if from.TableRefs.Right != nil {
		second := from.TableRefs.Right.(*ast.TableSource)
		secondTable := second.Source.(*ast.TableName)
		_, exists := s.globalTables[strings.ToLower(secondTable.Name.O)]
		if exists {
			if from.TableRefs.On != nil {
				vi := &ColumnNameVisitor{
					originTable:  firstTable.Name,
					rewriteTable: s.table,
				}
				from.TableRefs.On.Accept(vi)
				if s.fieldList != nil {
					s.fieldList.Accept(vi)
				}
				if s.where != nil {
					s.where.Accept(vi)
				}
				if s.orderBy != nil {
					s.orderBy.Accept(vi)
				}
			}
		}

		if secondAlgo, found := s.algorithms[strings.ToLower(secondTable.Name.O)]; found {
			if firstAlgo, exists := s.algorithms[strings.ToLower(firstTable.Name.O)]; exists {
				if firstAlgo.Equal(secondAlgo) {
					index := strings.LastIndex(s.table, "_")
					joinTable := fmt.Sprintf("%s_%s", secondAlgo.Topology().TableName, s.table[index+1:])

					if from.TableRefs.On != nil {
						visitor1 := &ColumnNameVisitor{
							originTable:  firstTable.Name,
							rewriteTable: s.table,
						}

						visitor2 := &ColumnNameVisitor{
							originTable:  secondTable.Name,
							rewriteTable: joinTable,
						}
						from.TableRefs.On.Accept(visitor1)
						from.TableRefs.On.Accept(visitor2)
						if s.fieldList != nil {
							s.fieldList.Accept(visitor1)
							s.fieldList.Accept(visitor2)
						}
						if s.where != nil {
							s.where.Accept(visitor1)
							s.where.Accept(visitor2)
						}
						if s.orderBy != nil {
							s.orderBy.Accept(visitor1)
							s.orderBy.Accept(visitor2)
						}
					}
					secondTable.Name = model.NewCIStr(joinTable)
				}
			}
		}
	}
	firstTable.Name = model.NewCIStr(s.table)
	return n, true
}

type ColumnRewriteVisitor struct {
	rewriteTable string
}

func (s *ColumnRewriteVisitor) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	return n, false
}

// Leave implement ast.Visitor
func (s *ColumnRewriteVisitor) Leave(n ast.Node) (node ast.Node, ok bool) {
	field, ok := n.(*ast.ColumnNameExpr)
	if !ok {
		return n, true
	}
	field.Name.Table = model.NewCIStr(s.rewriteTable)
	return n, true
}

type ColumnNameVisitor struct {
	originTable  model.CIStr
	rewriteTable string
}

func (s *ColumnNameVisitor) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	return n, false
}

// Leave implement ast.Visitor
func (s *ColumnNameVisitor) Leave(n ast.Node) (node ast.Node, ok bool) {
	field, ok := n.(*ast.ColumnNameExpr)
	if !ok {
		return n, true
	}
	if field.Name.Table.O == s.originTable.O {
		field.Name.Table = model.NewCIStr(s.rewriteTable)
	}
	return n, true
}
