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
	"strings"

	"github.com/pkg/errors"

	"github.com/cectc/dbpack/pkg/constant"
	"github.com/cectc/dbpack/pkg/dt"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/misc"
	"github.com/cectc/dbpack/pkg/mysql"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/pkg/visitor"
	"github.com/cectc/dbpack/third_party/parser"
	"github.com/cectc/dbpack/third_party/parser/ast"
	"github.com/cectc/dbpack/third_party/parser/format"
)

type UpdatePlan struct {
	Database string
	Tables   []string
	Stmt     *ast.UpdateStmt
	Args     []interface{}
	Executor proto.DBGroupExecutor
}

func (p *UpdatePlan) Execute(ctx context.Context, hints ...*ast.TableOptimizerHint) (proto.Result, uint16, error) {
	var (
		sb                     strings.Builder
		tx                     proto.Tx
		result                 proto.Result
		affectedRows, affected uint64
		warnings, warns        uint16
		err                    error
	)
	tx, _, err = p.Executor.Begin(ctx)
	if err != nil {
		return nil, 0, errors.WithStack(err)
	}
	for _, table := range p.Tables {
		sb.Reset()
		if err = p.generate(&sb, table, hints...); err != nil {
			return nil, 0, errors.Wrap(err, "failed to generate sql")
		}
		sql := sb.String()
		log.Debugf("update, db name: %s, sql: %s", p.Database, sql)

		_parser := parser.New()
		stmtNode, err := _parser.ParseOneStmt(sql, "", "")
		if err != nil {
			return nil, 0, errors.WithStack(err)
		}
		stmtNode.Accept(&visitor.ParamVisitor{})

		commandType := proto.CommandType(ctx)
		switch commandType {
		case constant.ComQuery:
			ctx := proto.WithQueryStmt(ctx, stmtNode)
			result, warns, err = tx.Query(ctx, sql)
		case constant.ComStmtExecute:
			stmt := generateStatement(sql, stmtNode, p.Args)
			ctx := proto.WithPrepareStmt(ctx, stmt)
			result, warns, err = tx.ExecuteSql(ctx, sql, p.Args...)
		default:
			continue
		}
		if err != nil {
			return nil, 0, errors.WithStack(err)
		}
		affected, err = result.RowsAffected()
		if err != nil {
			return nil, 0, errors.WithStack(err)
		}
		affectedRows += affected
		warnings += warns
	}
	_, err = tx.Commit(ctx)
	if err != nil {
		return nil, 0, err
	}
	mysqlResult := result.(*mysql.Result)
	mysqlResult.AffectedRows = affectedRows
	return mysqlResult, warnings, nil
}

func (p *UpdatePlan) generate(sb *strings.Builder, table string, hints ...*ast.TableOptimizerHint) error {
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, sb)
	ctx.WriteKeyWord("UPDATE ")

	if len(hints) != 0 {
		ctx.WritePlain("/*+ ")
		for i, tableHint := range hints {
			if i != 0 {
				ctx.WritePlain(" ")
			}
			if err := tableHint.Restore(ctx); err != nil {
				return errors.Wrapf(err, "An error occurred while restoring UpdateStmt.TableHints[%d], HintName: %s",
					i, tableHint.HintName.String())
			}
		}
		ctx.WritePlain("*/ ")
	}

	ctx.WritePlain(table)
	ctx.WriteKeyWord(" SET ")
	for i, assignment := range p.Stmt.List {
		if i != 0 {
			ctx.WritePlain(", ")
		}

		if err := assignment.Column.Restore(ctx); err != nil {
			return errors.Wrapf(err, "An error occur while restoring UpdateStmt.List[%d].Column", i)
		}

		ctx.WritePlain("=")

		if err := assignment.Expr.Restore(ctx); err != nil {
			return errors.Wrapf(err, "An error occur while restoring UpdateStmt.List[%d].Expr", i)
		}
	}

	if p.Stmt.Where != nil {
		ctx.WriteKeyWord(" WHERE ")
		if err := p.Stmt.Where.Restore(ctx); err != nil {
			return errors.Wrap(err, "An error occur while restoring UpdateStmt.Where")
		}
	}

	if p.Stmt.Order != nil {
		ctx.WritePlain(" ")
		if err := p.Stmt.Order.Restore(ctx); err != nil {
			return errors.Wrap(err, "An error occur while restoring UpdateStmt.Order")
		}
	}

	//if p.Stmt.Limit != nil {
	//	ctx.WritePlain(" ")
	//	if err := p.Stmt.Limit.Restore(ctx); err != nil {
	//		return errors.Wrap(err, "An error occur while restoring UpdateStmt.Limit")
	//	}
	//}
	return nil
}

type MultiUpdatePlan struct {
	Stmt  *ast.UpdateStmt
	Plans []*UpdatePlan
}

func (p *MultiUpdatePlan) Execute(ctx context.Context, _ ...*ast.TableOptimizerHint) (result proto.Result, warns uint16, err error) {
	var (
		affectedRows uint64
		warnings     uint16
		affected     uint64
		hints        []*ast.TableOptimizerHint
	)
	if has, _ := misc.HasXIDHint(p.Stmt.TableHints); !has {
		tableName := p.Stmt.TableRefs.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName).Name.String()
		transactionManager := dt.GetDistributedTransactionManager()
		timeoutVariable := proto.Variable(ctx, constant.TransactionTimeout)
		timeout, ok := timeoutVariable.(int32)
		if !ok {
			return nil, 0, errors.New("transaction timeout must be of type int32")
		}
		var xid string
		xid, err = transactionManager.Begin(ctx, fmt.Sprintf("UPDATE_%s", tableName), timeout)
		if err != nil {
			return nil, 0, err
		}
		hints = append(hints, misc.NewXIDHint(xid))
		defer func() {
			if err != nil {
				if _, rollbackErr := transactionManager.Rollback(ctx, xid); rollbackErr != nil {
					log.Error(err)
				}
			} else {
				if _, commitErr := transactionManager.Commit(ctx, xid); commitErr != nil {
					log.Error(err)
				}
			}
		}()
	}
	for _, pl := range p.Plans {
		result, warns, err = pl.Execute(ctx, hints...)
		if err != nil {
			return nil, 0, err
		}
		affected, err = result.RowsAffected()
		if err != nil {
			return nil, 0, errors.WithStack(err)
		}
		affectedRows += affected
		warnings += warns
	}
	return &mysql.Result{AffectedRows: affectedRows}, warnings, nil
}
