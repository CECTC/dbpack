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
//go:generate mockgen -destination=../../testdata/mock_db.go -package=testdata . DB
//go:generate mockgen -destination=../../testdata/mock_tx.go -package=testdata . Tx
package proto

import (
	"context"
	"time"

	"github.com/valyala/fasthttp"

	"github.com/cectc/dbpack/pkg/config"
	"github.com/cectc/dbpack/pkg/dt/api"
	"github.com/cectc/dbpack/third_party/parser/ast"
)

type (
	DBStatus uint8

	DistributedTransactionManager interface {
		Begin(ctx context.Context, transactionName string, timeout int32) (string, error)
		Commit(ctx context.Context, xid string) (api.GlobalSession_GlobalStatus, error)
		Rollback(ctx context.Context, xid string) (api.GlobalSession_GlobalStatus, error)
		BranchRegister(ctx context.Context, in *api.BranchRegisterRequest) (string, int64, error)
		BranchReport(ctx context.Context, branchID string, status api.BranchSession_BranchStatus) error
		ReleaseLockKeys(ctx context.Context, resourceID string, lockKeys []string) (bool, error)
		IsLockable(ctx context.Context, resourceID, lockKey string) (bool, error)
		IsLockableWithXID(ctx context.Context, resourceID, lockKey, xid string) (bool, error)
	}

	Listener interface {
		Listen()
		Close()
	}

	DBListener interface {
		Listener
		SetExecutor(executor Executor)
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

	Filter interface {
		GetKind() string
	}

	HttpPreFilter interface {
		Filter
		PreHandle(ctx context.Context, fastHttpCtx *fasthttp.RequestCtx) error
	}

	HttpPostFilter interface {
		Filter
		PostHandle(ctx context.Context, fastHttpCtx *fasthttp.RequestCtx) error
	}

	// DBPreFilter ...
	DBPreFilter interface {
		Filter
		PreHandle(ctx context.Context) error
	}

	// DBPostFilter ...
	DBPostFilter interface {
		Filter
		PostHandle(ctx context.Context, result Result, err error) error
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
		NewFilter(appid string, config map[string]interface{}) (Filter, error)
	}

	Connection interface {
		DataSourceName() string
		Connect(ctx context.Context) error
		Close()
	}

	DB interface {
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
		Ping() error
		Close()
		IsClosed() bool

		IsMaster() bool
		MasterName() string
		SetWriteWeight(int)
		SetReadWeight(int)
		WriteWeight() int
		ReadWeight() int

		SetConnectionPreFilters(filters []DBConnectionPreFilter)
		SetConnectionPostFilters(filters []DBConnectionPostFilter)

		UseDB(ctx context.Context, schema string) error
		ExecuteFieldList(ctx context.Context, table, wildcard string) ([]Field, error)
		Query(ctx context.Context, query string) (Result, uint16, error)
		QueryDirectly(query string) (Result, uint16, error)
		ExecuteStmt(ctx context.Context, stmt *Stmt) (Result, uint16, error)
		ExecuteSql(ctx context.Context, sql string, args ...interface{}) (Result, uint16, error)
		ExecuteSqlDirectly(sql string, args ...interface{}) (Result, uint16, error)
		Begin(ctx context.Context) (Tx, Result, error)
	}

	Tx interface {
		Query(ctx context.Context, query string) (Result, uint16, error)
		QueryDirectly(query string) (Result, uint16, error)
		ExecuteStmt(ctx context.Context, stmt *Stmt) (Result, uint16, error)
		ExecuteSql(ctx context.Context, sql string, args ...interface{}) (Result, uint16, error)
		ExecuteSqlDirectly(sql string, args ...interface{}) (Result, uint16, error)
		Commit(ctx context.Context) (Result, error)
		Rollback(ctx context.Context, stmt *ast.RollbackStmt) (Result, error)
		ReleaseSavepoint(ctx context.Context, savepoint string) (result Result, err error)
	}

	DBManager interface {
		GetDB(name string) DB
	}

	// DBGroupExecutor prepare a query, execute the statement, and then close the statement.
	DBGroupExecutor interface {
		GroupName() string
		Begin(ctx context.Context) (Tx, Result, error)
		Query(ctx context.Context, query string) (Result, uint16, error)
		QueryAll(ctx context.Context, query string) (Result, uint16, error)
		Execute(ctx context.Context, query string) (Result, uint16, error)
		PrepareQuery(ctx context.Context, query string, args ...interface{}) (Result, uint16, error)
		PrepareExecute(ctx context.Context, query string, args ...interface{}) (Result, uint16, error)
		PrepareExecuteStmt(ctx context.Context, stmt *Stmt) (Result, uint16, error)
	}

	DBGroupTx interface {
		Begin(ctx context.Context, executor DBGroupExecutor) (Tx, error)
		Query(ctx context.Context, query string) (Result, uint16, error)
		Execute(ctx context.Context, stmt ast.StmtNode, args ...interface{}) (Result, uint16, error)
		Commit(ctx context.Context) (result Result, err error)
		Rollback(ctx context.Context) (result Result, err error)
	}

	// Plan represents a plan for query/execute command.
	Plan interface {
		// Execute executes the current Plan.
		Execute(ctx context.Context, hints ...*ast.TableOptimizerHint) (Result, uint16, error)
	}

	// Optimizer represents a sql statement optimizer which can be used to create QueryPlan or ExecPlan.
	Optimizer interface {
		// Optimize optimizes the sql with arguments then returns a Plan.
		Optimize(ctx context.Context, stmt ast.StmtNode, args ...interface{}) (Plan, error)
	}
)

const (
	Unknown DBStatus = iota
	Running
)
