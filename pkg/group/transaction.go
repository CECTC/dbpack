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

package group

import (
	"context"

	"github.com/pkg/errors"
	"github.com/uber-go/atomic"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"

	err2 "github.com/cectc/dbpack/pkg/errors"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/mysql"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/pkg/tracing"
	"github.com/cectc/dbpack/third_party/parser/ast"
)

var (
	txID = atomic.NewUint32(0)
)

type ComplexTx struct {
	closed    *atomic.Bool
	id        uint32
	txs       map[string]proto.Tx
	optimizer proto.Optimizer
}

func NewComplexTx(optimizer proto.Optimizer) proto.DBGroupTx {
	txID.Inc()
	return &ComplexTx{
		closed:    atomic.NewBool(false),
		id:        txID.Load(),
		txs:       make(map[string]proto.Tx),
		optimizer: optimizer,
	}
}

func (tx *ComplexTx) Query(ctx context.Context, query string) (proto.Result, uint16, error) {
	spanCtx, span := tracing.GetTraceSpan(ctx, tracing.GroupQuery)
	defer span.End()

	queryStmt := proto.QueryStmt(spanCtx)
	if queryStmt == nil {
		return nil, 0, errors.New("query stmt should not be nil")
	}
	plan, err := tx.optimizer.Optimize(spanCtx, queryStmt)
	if err != nil {
		return nil, 0, err
	}
	txCtx := proto.WithDBGroupTx(spanCtx, tx)
	return plan.Execute(txCtx)
}

func (tx *ComplexTx) Execute(ctx context.Context, stmt ast.StmtNode, args ...interface{}) (proto.Result, uint16, error) {
	spanCtx, span := tracing.GetTraceSpan(ctx, tracing.GroupExecute)
	defer span.End()

	plan, err := tx.optimizer.Optimize(spanCtx, stmt, args...)
	if err != nil {
		return nil, 0, err
	}
	txCtx := proto.WithDBGroupTx(spanCtx, tx)
	return plan.Execute(txCtx)
}

func (tx *ComplexTx) Begin(ctx context.Context, executor proto.DBGroupExecutor) (proto.Tx, error) {
	spanCtx, span := tracing.GetTraceSpan(ctx, tracing.GroupTransactionBegin)
	span.SetAttributes(attribute.KeyValue{Key: "group", Value: attribute.StringValue(executor.GroupName())})
	defer span.End()

	if childTx, ok := tx.txs[executor.GroupName()]; ok {
		return childTx, nil
	}
	masterCtx := proto.WithMaster(spanCtx)
	childTx, _, err := executor.Begin(masterCtx)
	if err != nil {
		return nil, err
	}
	log.Debugf("DBGroup %s has begun local transaction!", executor.GroupName())
	tx.txs[executor.GroupName()] = childTx
	return childTx, nil
}

func (tx *ComplexTx) Commit(ctx context.Context) (result proto.Result, err error) {
	spanCtx, span := tracing.GetTraceSpan(ctx, tracing.GroupTxCommit)
	defer span.End()

	if tx.closed.Load() {
		return nil, err2.ErrTransactionClosed
	}
	defer tx.Close()

	var g errgroup.Group
	for group, child := range tx.txs {
		// https://golang.org/doc/faq#closures_and_goroutines
		group, child := group, child
		g.Go(func() error {
			_, err := child.Commit(spanCtx)
			if err != nil {
				log.Errorf("commit failed, db group: %s, err: %v", group, err)
				return err
			}
			log.Debugf("DBGroup %s has committed local transaction!", group)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}
	return &mysql.Result{
		AffectedRows: 0,
		InsertId:     0,
	}, nil
}

func (tx *ComplexTx) Rollback(ctx context.Context) (result proto.Result, err error) {
	spanCtx, span := tracing.GetTraceSpan(ctx, tracing.GroupTxCommit)
	defer span.End()

	if tx.closed.Load() {
		return nil, err2.ErrTransactionClosed
	}
	defer tx.Close()

	var g errgroup.Group
	for group, child := range tx.txs {
		group, child := group, child
		g.Go(func() error {
			_, err := child.Rollback(spanCtx, nil)
			if err != nil {
				log.Errorf("rollback failed, db group: %s, err: %v", group, err)
				return err
			}
			log.Debugf("DBGroup %s has rollbacked local transaction!", group)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}
	return &mysql.Result{
		AffectedRows: 0,
		InsertId:     0,
	}, nil
}

func (tx *ComplexTx) Close() {
	tx.closed.Swap(true)
	tx.txs = nil
}
