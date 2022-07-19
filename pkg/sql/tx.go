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

package sql

import (
	"context"
	"fmt"

	"github.com/uber-go/atomic"
	"go.opentelemetry.io/otel/attribute"

	"github.com/cectc/dbpack/pkg/driver"
	err2 "github.com/cectc/dbpack/pkg/errors"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/pkg/tracing"
)

type Tx struct {
	closed *atomic.Bool
	db     *DB
	conn   *driver.BackendConnection
}

func (tx *Tx) Query(ctx context.Context, query string) (proto.Result, uint16, error) {
	spanCtx, span := tracing.GetTraceSpan(ctx, tracing.TxQuery)
	span.SetAttributes(attribute.KeyValue{Key: "db", Value: attribute.StringValue(tx.db.name)},
		attribute.KeyValue{Key: "sql", Value: attribute.StringValue(query)})
	defer span.End()

	tx.db.inflightRequests.Inc()
	defer tx.db.inflightRequests.Dec()

	if err := tx.db.doConnectionPreFilter(spanCtx, tx.conn); err != nil {
		return nil, 0, err
	}
	result, warn, err := tx.conn.ExecuteWithWarningCount(query, true)
	if err != nil {
		return result, warn, err
	}
	if err := tx.db.doConnectionPostFilter(spanCtx, result, tx.conn); err != nil {
		return nil, 0, err
	}
	return result, warn, err
}

func (tx *Tx) ExecuteStmt(ctx context.Context, stmt *proto.Stmt) (proto.Result, uint16, error) {
	query := stmt.StmtNode.Text()
	spanCtx, span := tracing.GetTraceSpan(ctx, tracing.TxExecStmt)
	span.SetAttributes(attribute.KeyValue{Key: "db", Value: attribute.StringValue(tx.db.name)},
		attribute.KeyValue{Key: "sql", Value: attribute.StringValue(query)})
	defer span.End()

	tx.db.inflightRequests.Inc()
	defer tx.db.inflightRequests.Dec()

	if err := tx.db.doConnectionPreFilter(spanCtx, tx.conn); err != nil {
		return nil, 0, err
	}

	var (
		result proto.Result
		args   []interface{}
		warn   uint16
		err    error
	)
	if stmt.HasLongDataParam {
		for i := 0; i < len(stmt.BindVars); i++ {
			parameterID := fmt.Sprintf("v%d", i+1)
			args = append(args, stmt.BindVars[parameterID])
		}
		result, warn, err = tx.conn.PrepareQueryArgs(query, args)
	} else {
		result, warn, err = tx.conn.PrepareQuery(query, stmt.ParamData)
	}
	if err != nil {
		return result, warn, err
	}
	if err := tx.db.doConnectionPostFilter(spanCtx, result, tx.conn); err != nil {
		return nil, 0, err
	}
	return result, warn, err
}

func (tx *Tx) ExecuteSql(ctx context.Context, sql string, args ...interface{}) (proto.Result, uint16, error) {
	spanCtx, span := tracing.GetTraceSpan(ctx, tracing.TxExecSQL)
	span.SetAttributes(attribute.KeyValue{Key: "db", Value: attribute.StringValue(tx.db.name)},
		attribute.KeyValue{Key: "sql", Value: attribute.StringValue(sql)})
	defer span.End()

	tx.db.inflightRequests.Inc()
	defer tx.db.inflightRequests.Dec()

	if err := tx.db.doConnectionPreFilter(spanCtx, tx.conn); err != nil {
		return nil, 0, err
	}
	result, warn, err := tx.conn.PrepareQueryArgs(sql, args)
	if err != nil {
		return result, warn, err
	}
	if err := tx.db.doConnectionPostFilter(spanCtx, result, tx.conn); err != nil {
		return nil, 0, err
	}
	return result, warn, err
}

func (tx *Tx) Commit(ctx context.Context) (result proto.Result, err error) {
	_, span := tracing.GetTraceSpan(ctx, tracing.TxCommit)
	span.SetAttributes(attribute.KeyValue{Key: "db", Value: attribute.StringValue(tx.db.name)})
	defer span.End()

	if tx.closed.Load() {
		return nil, nil
	}
	if tx.db == nil || tx.db.IsClosed() {
		return nil, err2.ErrInvalidConn
	}
	result, err = tx.conn.Execute("COMMIT", false)
	tx.db.pool.Put(tx.conn)
	tx.Close()
	return
}

func (tx *Tx) Rollback(ctx context.Context) (result proto.Result, err error) {
	_, span := tracing.GetTraceSpan(ctx, tracing.TxRollback)
	span.SetAttributes(attribute.KeyValue{Key: "db", Value: attribute.StringValue(tx.db.name)})
	defer span.End()

	if tx.closed.Load() {
		return nil, nil
	}
	if tx.db == nil || tx.db.IsClosed() {
		return nil, err2.ErrInvalidConn
	}
	result, err = tx.conn.Execute("ROLLBACK", false)
	tx.db.pool.Put(tx.conn)
	tx.Close()
	return
}

func (tx *Tx) Close() {
	tx.closed.Swap(true)
	tx.db = nil
	tx.conn = nil
}
