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
	"time"

	"github.com/pingcap/errors"
	"github.com/uber-go/atomic"

	"github.com/cectc/dbpack/pkg/driver"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/third_party/pools"
)

type DB struct {
	name                     string
	pingInterval             time.Duration
	pingTimesForChangeStatus int
	status                   proto.DBStatus
	pool                     *pools.ResourcePool

	connectionPreFilters  []proto.DBConnectionPreFilter
	connectionPostFilters []proto.DBConnectionPostFilter

	inflightRequests *atomic.Int64
	pingCount        *atomic.Int64
}

func NewDB(name string, pingInterval time.Duration,
	pingTimesForChangeStatus int, pool *pools.ResourcePool) proto.DB {
	db := &DB{
		name:                     name,
		pingInterval:             pingInterval,
		pingTimesForChangeStatus: pingTimesForChangeStatus,
		status:                   proto.Running,
		pool:                     pool,
		inflightRequests:         atomic.NewInt64(0),
		pingCount:                atomic.NewInt64(0),
	}
	go db.Ping()
	return db
}

func (db *DB) UseDB(ctx context.Context, schema string) error {
	db.inflightRequests.Inc()
	defer db.inflightRequests.Dec()

	r, err := db.pool.Get(ctx)
	if err != nil {
		err = errors.WithStack(err)
		return err
	}
	defer db.pool.Put(r)

	conn := r.(*driver.BackendConnection)
	return conn.WriteComInitDB(schema)
}

func (db *DB) ExecuteFieldList(ctx context.Context, table, wildcard string) ([]proto.Field, error) {
	db.inflightRequests.Inc()
	defer db.inflightRequests.Dec()

	r, err := db.pool.Get(ctx)
	if err != nil {
		err = errors.WithStack(err)
		return nil, err
	}
	defer db.pool.Put(r)

	conn := r.(*driver.BackendConnection)
	if err := conn.WriteComFieldList(table, wildcard); err != nil {
		return nil, err
	}

	fields, err := conn.ReadColumnDefinitions()
	if err != nil {
		return nil, err
	}

	result := make([]proto.Field, 0, len(fields))
	for i, field := range fields {
		result[i] = field
	}
	return result, nil
}

func (db *DB) Query(ctx context.Context, query string) (proto.Result, uint16, error) {
	db.inflightRequests.Inc()
	defer db.inflightRequests.Dec()

	r, err := db.pool.Get(ctx)
	if err != nil {
		err = errors.WithStack(err)
		return nil, 0, err
	}
	defer db.pool.Put(r)

	conn := r.(*driver.BackendConnection)
	if err := db.doConnectionPreFilter(ctx, conn); err != nil {
		return nil, 0, err
	}
	result, warn, err := conn.ExecuteWithWarningCount(query, true)
	if err != nil {
		return result, warn, err
	}
	if err := db.doConnectionPostFilter(ctx, result, conn); err != nil {
		return nil, 0, err
	}
	return result, warn, err
}

func (db *DB) ExecuteStmt(ctx context.Context, stmt *proto.Stmt) (proto.Result, uint16, error) {
	db.inflightRequests.Inc()
	defer db.inflightRequests.Dec()

	r, err := db.pool.Get(ctx)
	if err != nil {
		err = errors.WithStack(err)
		return nil, 0, err
	}
	defer db.pool.Put(r)

	conn := r.(*driver.BackendConnection)
	query := stmt.StmtNode.Text()
	if err := db.doConnectionPreFilter(ctx, conn); err != nil {
		return nil, 0, err
	}
	result, warn, err := conn.PrepareQuery(query, stmt.ParamData)
	if err != nil {
		return result, warn, err
	}
	if err := db.doConnectionPostFilter(ctx, result, conn); err != nil {
		return nil, 0, err
	}
	return result, warn, err
}

func (db *DB) ExecuteSql(ctx context.Context, sql string, args ...interface{}) (proto.Result, uint16, error) {
	db.inflightRequests.Inc()
	defer db.inflightRequests.Dec()

	r, err := db.pool.Get(ctx)
	if err != nil {
		err = errors.WithStack(err)
		return nil, 0, err
	}
	defer db.pool.Put(r)
	conn := r.(*driver.BackendConnection)
	if err := db.doConnectionPreFilter(ctx, conn); err != nil {
		return nil, 0, err
	}
	result, warn, err := conn.PrepareQueryArgs(sql, args)
	if err != nil {
		return result, warn, err
	}
	if err := db.doConnectionPostFilter(ctx, result, conn); err != nil {
		return nil, 0, err
	}
	return result, warn, err
}

func (db *DB) Begin(ctx context.Context) (proto.Tx, proto.Result, error) {
	var (
		result proto.Result
		conn   *driver.BackendConnection
		err    error
	)

	r, err := db.pool.Get(ctx)
	if err != nil {
		err = errors.WithStack(err)
		return nil, nil, err
	}
	conn = r.(*driver.BackendConnection)

	if result, err = conn.Execute("START TRANSACTION", false); err != nil {
		db.pool.Put(r)
		return nil, nil, err
	}

	return &Tx{
		closed: atomic.NewBool(false),
		db:     db,
		conn:   conn,
	}, result, nil
}

func (db *DB) SetConnectionPreFilters(filters []proto.DBConnectionPreFilter) {
	db.connectionPreFilters = filters
}

func (db *DB) SetConnectionPostFilters(filters []proto.DBConnectionPostFilter) {
	db.connectionPostFilters = filters
}

func (db *DB) doConnectionPreFilter(ctx context.Context, conn proto.Connection) error {
	for i := 0; i < len(db.connectionPreFilters); i++ {
		f := db.connectionPreFilters[i]
		err := f.PreHandle(ctx, conn)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) doConnectionPostFilter(ctx context.Context, result proto.Result, conn proto.Connection) error {
	for i := 0; i < len(db.connectionPostFilters); i++ {
		f := db.connectionPostFilters[i]
		err := f.PostHandle(ctx, result, conn)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) Name() string {
	return db.name
}

func (db *DB) Status() proto.DBStatus {
	return db.status
}

func (db *DB) SetCapacity(capacity int) error {
	return db.pool.SetCapacity(capacity)
}

func (db *DB) SetIdleTimeout(idleTimeout time.Duration) {
	db.pool.SetIdleTimeout(idleTimeout)
}

// Capacity returns the capacity.
func (db *DB) Capacity() int64 {
	return db.pool.Capacity()
}

// Available returns the number of currently unused and available connections.
func (db *DB) Available() int64 {
	return db.pool.Available()
}

// Active returns the number of active (i.e. non-nil) connections either in the
// pool or claimed for use
func (db *DB) Active() int64 {
	return db.pool.Active()
}

// InUse returns the number of claimed connections from the pool
func (db *DB) InUse() int64 {
	return db.pool.InUse()
}

// MaxCap returns the max capacity.
func (db *DB) MaxCap() int64 {
	return db.pool.MaxCap()
}

// WaitCount returns the total number of waits.
func (db *DB) WaitCount() int64 {
	return db.pool.WaitCount()
}

// WaitTime returns the total wait time.
func (db *DB) WaitTime() time.Duration {
	return db.pool.WaitTime()
}

// IdleTimeout returns the idle timeout.
func (db *DB) IdleTimeout() time.Duration {
	return db.pool.IdleTimeout()
}

// IdleClosed returns the count of connections closed due to idle timeout.
func (db *DB) IdleClosed() int64 {
	return db.pool.IdleClosed()
}

// Exhausted returns the number of times Available dropped below 1
func (db *DB) Exhausted() int64 {
	return db.pool.Exhausted()
}

// StatsJSON returns the stats in JSON format.
func (db *DB) StatsJSON() string {
	return db.pool.StatsJSON()
}

func (db *DB) Ping() {
	timer := time.NewTimer(db.pingInterval)
	for {
		<-timer.C
		err := db.ping()
		if err != nil {
			log.Errorf("db %s ping failed", db.name)
		}
		timer.Reset(db.pingInterval)
	}
}

func (db *DB) ping() (err error) {
	defer func() {
		if db.status == proto.Running {
			if err != nil {
				db.pingCount.Inc()
			} else {
				db.pingCount.Dec()
			}
		} else {
			if err == nil {
				db.pingCount.Inc()
			} else {
				db.pingCount.Dec()
			}
		}
		currentCount := db.pingCount.Load()
		if currentCount%int64(db.pingTimesForChangeStatus) == 0 {
			db.pingCount.Swap(0)
			if currentCount > 0 {
				db.status |= db.status
			}
		}
	}()
	r, err := db.pool.Get(context.Background())
	if err != nil {
		return err
	}
	defer db.pool.Put(r)
	conn := r.(*driver.BackendConnection)
	err = conn.Ping(context.Background())
	return
}

func (db *DB) Close() {
	for db.inflightRequests.Load() == 0 {
		db.pool.Close()
	}
}

// IsClosed returns true if the db is closed.
func (db *DB) IsClosed() (closed bool) {
	return db.pool.IsClosed()
}
