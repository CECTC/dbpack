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

//go:generate mockgen -destination=../../testdata/mock_db_manager.go -package=testdata . DBManager
package proto

import (
	"context"
	"time"

	"github.com/valyala/fasthttp"

	"github.com/cectc/dbpack/pkg/config"
)

type (
	DBStatus uint8

	Listener interface {
		Listen()

		Close()
	}

	DBListener interface {
		Listener
		SetExecutor(executor Executor)
	}

	Connection interface {
		DataSourceName() string
		Connect(ctx context.Context) error
		Close()
	}

	Filter interface {
		GetName() string
	}

	HttpPreFilter interface {
		Filter
		PreHandle(ctx *fasthttp.RequestCtx) error
	}

	HttpPostFilter interface {
		Filter
		PostHandle(ctx *fasthttp.RequestCtx) error
	}

	// DBPreFilter ...
	DBPreFilter interface {
		Filter
		PreHandle(ctx context.Context) error
	}

	// DBPostFilter ...
	DBPostFilter interface {
		Filter
		PostHandle(ctx context.Context, result Result) error
	}

	DBConnectionPreFilter interface {
		Filter
		PreHandle(ctx context.Context, conn Connection) error
	}

	DBConnectionPostFilter interface {
		Filter
		PostHandle(ctx context.Context, result Result, conn Connection) error
	}

	FilterFactory interface {
		NewFilter(config map[string]interface{}) (Filter, error)
	}

	DB interface {
		UseDB(ctx context.Context, schema string) error
		ExecuteFieldList(ctx context.Context, table, wildcard string) ([]Field, error)
		Query(ctx context.Context, query string) (Result, uint16, error)
		ExecuteStmt(ctx context.Context, stmt *Stmt) (Result, uint16, error)
		ExecuteSql(ctx context.Context, sql string, args ...interface{}) (Result, uint16, error)
		Begin(ctx context.Context) (Tx, Result, error)
		SetConnectionPreFilters(filters []DBConnectionPreFilter)
		SetConnectionPostFilters(filters []DBConnectionPostFilter)
		Name() string
		Status() DBStatus
		SetCapacity(capacity int) error
		SetIdleTimeout(idleTimeout time.Duration)
		Capacity() int64
		Available() int64
		Active() int64
		InUse() int64
		MaxCap() int64
		WaitCount() int64
		WaitTime() time.Duration
		IdleTimeout() time.Duration
		IdleClosed() int64
		Exhausted() int64
		StatsJSON() string
		Ping()
		Close()
		IsClosed() (closed bool)
	}

	Tx interface {
		Query(ctx context.Context, query string) (Result, uint16, error)
		ExecuteStmt(ctx context.Context, stmt *Stmt) (Result, uint16, error)
		ExecuteSql(ctx context.Context, sql string, args ...interface{}) (Result, uint16, error)
		Commit(ctx context.Context) (Result, error)
		Rollback(ctx context.Context) (Result, error)
	}

	// Executor ...
	Executor interface {
		GetPreFilters() []DBPreFilter

		GetPostFilters() []DBPostFilter

		ExecuteMode() config.ExecuteMode

		ProcessDistributedTransaction() bool

		InLocalTransaction(ctx context.Context) bool

		InGlobalTransaction(ctx context.Context) bool

		ExecuteUseDB(ctx context.Context, db string) error

		ExecuteFieldList(ctx context.Context, table, wildcard string) ([]Field, error)

		ExecutorComQuery(ctx context.Context, sql string) (Result, uint16, error)

		ExecutorComStmtExecute(ctx context.Context, stmt *Stmt) (Result, uint16, error)

		ConnectionClose(ctx context.Context)
	}

	DBManager interface {
		GetDB(name string) DB
	}
)

const (
	Unknown DBStatus = iota
	Running
)
