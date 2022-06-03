package plan

import (
	"context"
	"strings"

	"github.com/pkg/errors"

	"github.com/cectc/dbpack/pkg/constant"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/mysql"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/third_party/parser/ast"
	"github.com/cectc/dbpack/third_party/parser/format"
)

type DeleteOnSingleDBPlan struct {
	Database string
	Tables   []string
	Stmt     *ast.DeleteStmt
	Args     []interface{}
	Executor proto.DBGroupExecutor
}

func (p *DeleteOnSingleDBPlan) Execute(ctx context.Context) (proto.Result, uint16, error) {
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
		if err = p.generate(&sb, table); err != nil {
			return nil, 0, errors.Wrap(err, "failed to generate sql for delete")
		}
		sql := sb.String()
		log.Debugf("delete, db name: %s, sql: %s", p.Database, sql)

		commandType := proto.CommandType(ctx)
		switch commandType {
		case constant.ComQuery:
			result, warns, err = tx.Query(ctx, sql)
		case constant.ComStmtExecute:
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

func (p *DeleteOnSingleDBPlan) generate(sb *strings.Builder, table string) error {
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, sb)
	ctx.WriteKeyWord("DELETE ")
	ctx.WriteKeyWord("FROM ")
	// todo add xid hint for distributed transaction
	ctx.WritePlain(table)
	if p.Stmt.Where != nil {
		ctx.WriteKeyWord(" WHERE ")
		if err := p.Stmt.Where.Restore(ctx); err != nil {
			return errors.Wrap(err, "An error occurred while restore DeleteStmt.Where")
		}
	}

	if p.Stmt.Order != nil {
		ctx.WritePlain(" ")
		if err := p.Stmt.Order.Restore(ctx); err != nil {
			return errors.Wrap(err, "An error occurred while restore DeleteStmt.Order")
		}
	}

	//if p.Stmt.Limit != nil {
	//	ctx.WritePlain(" ")
	//	if err := p.Stmt.Limit.Restore(ctx); err != nil {
	//		return errors.Wrap(err, "An error occurred while restore DeleteStmt.Limit")
	//	}
	//}

	return nil
}

type DeleteOnMultiDBPlan struct {
	Stmt  *ast.DeleteStmt
	Plans []*DeleteOnSingleDBPlan
}

func (p *DeleteOnMultiDBPlan) Execute(ctx context.Context) (proto.Result, uint16, error) {
	var (
		affectedRows uint64
		warnings     uint16
	)
	// todo distributed transaction
	for _, pl := range p.Plans {
		result, warns, err := pl.Execute(ctx)
		if err != nil {
			return nil, 0, err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return nil, 0, errors.WithStack(err)
		}
		affectedRows += affected
		warnings += warns
	}
	return &mysql.Result{AffectedRows: affectedRows}, warnings, nil
}
