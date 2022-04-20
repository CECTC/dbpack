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

package mysql

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	_ "github.com/go-sql-driver/mysql" // register mysql
	"github.com/go-xorm/xorm"
	"xorm.io/builder"

	"github.com/cectc/dbpack/pkg/dt/api"
	"github.com/cectc/dbpack/pkg/dt/schema"
	"github.com/cectc/dbpack/pkg/dt/storage"
	"github.com/cectc/dbpack/pkg/dt/storage/factory"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/misc"
)

const (
	InsertGlobalTransaction = `insert into %s (addressing, xid, transaction_id, transaction_name, timeout, begin_time,
		status, active, gmt_create, gmt_modified) values(?, ?, ?, ?, ?, ?, ?, ?, now(), now())`

	QueryGlobalTransactionByXid = `select addressing, xid, transaction_id, transaction_name, timeout, begin_time,
		status, active, gmt_create, gmt_modified from %s where xid = ?`

	CountGlobalTransactions = `select count(1) from %s where addressing = ?`

	UpdateGlobalTransaction = "update %s set status = ?, gmt_modified = now() where xid = ?"

	InactiveGlobalTransaction = "update %s set active = 0, gmt_modified = now() where xid = ?"

	DeleteGlobalTransaction = "delete from %s where xid = ?"

	InsertBranchTransaction = `insert into %s (addressing, xid, branch_id, transaction_id, resource_id, lock_key, branch_type,
        status, application_data, async, gmt_create, gmt_modified) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now(), now())`

	QueryBranchTransaction = `select addressing, xid, branch_id, transaction_id, resource_id, lock_key, branch_type, status,
	    application_data, async, gmt_create, gmt_modified from %s where %s order by gmt_create asc`

	QueryBranchTransactionByXID = `select addressing, xid, branch_id, transaction_id, resource_id, lock_key, branch_type, status,
	    application_data, async, gmt_create, gmt_modified from %s where xid = ? order by gmt_create asc`

	QueryBranchTransactionByBranchID = `select addressing, xid, branch_id, transaction_id, resource_id, lock_key, branch_type, status,
	    application_data, async, gmt_create, gmt_modified from %s where branch_id = ? order by gmt_create asc`

	UpdateBranchTransaction = "update %s set status = ?, gmt_modified = now() where xid = ? and branch_id = ?"

	DeleteBranchTransaction = "delete from %s where xid = ? and branch_id = ?"

	DeleteBranchTransactionByResourceID = "delete from %s where xid = ? and resource_id = ?"

	InsertRowLock = `insert into %s (xid, branch_id, row_key, gmt_create, gmt_modified) values %s`

	QueryRowKey = `select xid, branch_id, row_key, gmt_create, gmt_modified from %s where %s order by gmt_create asc`

	DeleteRowKey = `delete from %s where %s`

	DeleteRowKeyByXID = `delete from %s where xid = ?`

	DeleteRowKeyByBranchID = `delete from %s where branch_id = ?`
)

func init() {
	factory.Register("mysql", &mysqlFactory{})
}

type DriverParameters struct {
	DSN                string        `yaml:"dsn" json:"dsn"`
	GlobalTable        string        `yaml:"global_table" json:"global_table"`
	BranchTable        string        `yaml:"branch_table" json:"branch_table"`
	LockTable          string        `yaml:"lock_table" json:"lock_table"`
	QueryLimit         int           `yaml:"query_limit" json:"query_limit"`
	MaxOpenConnections int           `yaml:"max_open_connections" json:"max_open_connections"`
	MaxIdleConnections int           `yaml:"max_idle_connections" json:"max_idle_connections"`
	MaxLifeTime        time.Duration `yaml:"max_lifetime" json:"-"`
	MaxLifeTimeStr     string        `yaml:"-" json:"max_lifetime"`
}

// mysqlFactory implements the factory.StorageDriverFactory interface
type mysqlFactory struct{}

func (factory *mysqlFactory) Create(parameters map[string]interface{}) (storage.Driver, error) {
	return FromParameters(parameters)
}

type driver struct {
	engine      *xorm.Engine
	globalTable string
	branchTable string
	lockTable   string
	queryLimit  int
}

func FromParameters(parameters map[string]interface{}) (storage.Driver, error) {
	var (
		err              error
		content          []byte
		driverParameters DriverParameters
	)

	if content, err = json.Marshal(parameters); err != nil {
		return nil, errors.Wrap(err, "marshal distributed transaction storage driver parameters failed.")
	}
	if err = json.Unmarshal(content, &driverParameters); err != nil {
		return nil, errors.Wrap(err, "unmarshal distributed transaction storage driver parameters failed.")
	}
	if driverParameters.MaxLifeTime, err = time.ParseDuration(driverParameters.MaxLifeTimeStr); err != nil {
		return nil, errors.Wrap(err, "the max_lifetime parameter should be a duration")
	}

	return New(driverParameters)
}

// New constructs a new Driver
func New(params DriverParameters) (storage.Driver, error) {
	if params.DSN == "" {
		return nil, fmt.Errorf("the dsn parameter should not be empty")
	}
	engine, err := xorm.NewEngine("mysql", params.DSN)
	if err != nil {
		return nil, err
	}
	engine.SetMaxOpenConns(params.MaxOpenConnections)
	engine.SetMaxIdleConns(params.MaxIdleConnections)
	engine.SetConnMaxLifetime(params.MaxLifeTime)

	return &driver{
		engine:      engine,
		globalTable: params.GlobalTable,
		branchTable: params.BranchTable,
		lockTable:   params.LockTable,
		queryLimit:  params.QueryLimit,
	}, nil
}

// AddGlobalSession add global session.
func (driver *driver) AddGlobalSession(session *api.GlobalSession) error {
	_, err := driver.engine.Exec(fmt.Sprintf(InsertGlobalTransaction, driver.globalTable),
		session.Addressing, session.XID, session.TransactionID, session.TransactionName,
		session.Timeout, session.BeginTime, session.Status, session.Active)
	return err
}

// FindGlobalSession find global session.
func (driver *driver) FindGlobalSession(xid string) *api.GlobalSession {
	var globalTransaction api.GlobalSession
	_, err := driver.engine.SQL(fmt.Sprintf(QueryGlobalTransactionByXid, driver.globalTable), xid).
		Get(&globalTransaction)
	if err != nil {
		log.Errorf(err.Error())
	}
	return &globalTransaction
}

func (driver *driver) FindGlobalSessions(statuses []api.GlobalSession_GlobalStatus, addressing string) []*api.GlobalSession {
	var globalSessions []*api.GlobalSession
	err := driver.engine.Table(driver.globalTable).
		Where(builder.
			In("status", statuses).
			And(builder.Eq{"addressing": addressing})).
		OrderBy("gmt_modified").
		Limit(driver.queryLimit).
		Find(&globalSessions)

	if err != nil {
		log.Errorf(err.Error())
	}
	return globalSessions
}

func (driver *driver) CountGlobalSessions(addressing string) (int, error) {
	var count int
	_, err := driver.engine.SQL(fmt.Sprintf(CountGlobalTransactions, driver.globalTable), addressing).Get(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// UpdateGlobalSessionStatus update global session status.
func (driver *driver) UpdateGlobalSessionStatus(session *api.GlobalSession, status api.GlobalSession_GlobalStatus) error {
	_, err := driver.engine.Exec(fmt.Sprintf(UpdateGlobalTransaction, driver.globalTable), status, session.XID)
	return err
}

// CanBeAsyncCommitOrRollback ...
func (driver *driver) CanBeAsyncCommitOrRollback(session *api.GlobalSession) bool {
	var branchTransactions []*api.BranchSession
	whereCond := "xid = ? and async = false"
	err := driver.engine.SQL(fmt.Sprintf(QueryBranchTransaction, driver.branchTable, whereCond), session.XID).Find(&branchTransactions)
	if err != nil {
		log.Errorf(err.Error())
	}
	if len(branchTransactions) == 0 {
		return true
	}
	return false
}

// InactiveGlobalSession inactive global session.
func (driver *driver) InactiveGlobalSession(session *api.GlobalSession) error {
	_, err := driver.engine.Exec(fmt.Sprintf(InactiveGlobalTransaction, driver.globalTable), session.XID)
	return err
}

// ReleaseGlobalSessionLock inactive global session.
func (driver *driver) ReleaseGlobalSessionLock(session *api.GlobalSession) error {
	_, err := driver.engine.Exec(fmt.Sprintf(DeleteRowKeyByXID, driver.lockTable), session.XID)
	return err
}

// RemoveGlobalSession remove global session.
func (driver *driver) RemoveGlobalSession(session *api.GlobalSession) error {
	_, err := driver.engine.Exec(fmt.Sprintf(DeleteGlobalTransaction, driver.globalTable), session.XID)
	return err
}

// AddBranchSession add branch session.
func (driver *driver) AddBranchSession(globalSession *api.GlobalSession, session *api.BranchSession) error {
	if session.Type == api.AT {
		if !driver.AcquireLock(session) {
			return errors.New("acquire lock failed")
		}
	}
	_, err := driver.engine.Exec(fmt.Sprintf(InsertBranchTransaction, driver.branchTable),
		session.Addressing, session.XID, session.BranchID, session.TransactionID, session.ResourceID, session.LockKey,
		session.Type, session.Status, session.ApplicationData, session.Async)
	return err
}

func (driver *driver) AcquireLock(session *api.BranchSession) bool {
	locks := schema.CollectRowLocks(session.LockKey, session.ResourceID, session.XID, session.BranchID)
	locks, rowKeyArgs := distinctByKey(locks)
	var existedRowLocks []*schema.RowLock
	whereCond := fmt.Sprintf("row_key in %s", misc.MysqlAppendInParam(len(rowKeyArgs)))
	err := driver.engine.SQL(fmt.Sprintf(QueryRowKey, driver.lockTable, whereCond), rowKeyArgs...).Find(&existedRowLocks)
	if err != nil {
		log.Errorf(err.Error())
	}

	var unrepeatedLocks = make([]*schema.RowLock, 0)
	if !session.SkipCheckLock {
		currentXID := locks[0].XID
		canLock := true
		existedRowKeys := make([]string, 0)
		for _, rowLock := range existedRowLocks {
			if rowLock.XID != currentXID {
				log.Infof("row lock [%s] is holding by xid {%s} branchID {%d}", rowLock.RowKey, rowLock.XID, rowLock.BranchID)
				canLock = false
				break
			}
			existedRowKeys = append(existedRowKeys, rowLock.RowKey)
		}
		if !canLock {
			return false
		}
		if len(existedRowKeys) > 0 {
			for _, lock := range locks {
				if !contains(existedRowKeys, lock.RowKey) {
					unrepeatedLocks = append(unrepeatedLocks, lock)
				}
			}
		} else {
			unrepeatedLocks = locks
		}
		if len(unrepeatedLocks) == 0 {
			return true
		}
	}

	if len(unrepeatedLocks) == 0 {
		unrepeatedLocks = locks
	}
	var (
		sb        strings.Builder
		args      []interface{}
		sqlOrArgs []interface{}
	)
	for i := 0; i < len(unrepeatedLocks); i++ {
		sb.WriteString("(?, ?, ?, now(), now()),")
		args = append(args, unrepeatedLocks[i].XID, unrepeatedLocks[i].BranchID, unrepeatedLocks[i].RowKey)
	}
	values := sb.String()
	valueStr := values[:len(values)-1]

	sqlOrArgs = append(sqlOrArgs, fmt.Sprintf(InsertRowLock, driver.lockTable, valueStr))
	sqlOrArgs = append(sqlOrArgs, args...)
	_, err = driver.engine.Exec(sqlOrArgs...)
	if err != nil {
		// In an extremely high concurrency scenario, the row lock has been written to the database,
		// but the mysql driver reports invalid connection exception, and then re-registers the branch,
		// it will report the duplicate key exception.
		log.Errorf("row locks batch acquire failed, %v, %v", unrepeatedLocks, err)
		return false
	}
	return true
}

// FindBranchSessions find branch session.
func (driver *driver) FindBranchSessions(xid string) []*api.BranchSession {
	var branchTransactions []*api.BranchSession
	err := driver.engine.SQL(fmt.Sprintf(QueryBranchTransactionByXID, driver.branchTable), xid).Find(&branchTransactions)
	if err != nil {
		log.Errorf(err.Error())
	}
	return branchTransactions
}

func (driver *driver) FindBranchSessionByBranchID(branchID int64) *api.BranchSession {
	var branchTransaction *api.BranchSession
	_, err := driver.engine.SQL(fmt.Sprintf(QueryBranchTransactionByBranchID, driver.branchTable), branchID).Get(branchTransaction)
	if err != nil {
		log.Errorf(err.Error())
	}
	return branchTransaction
}

func (driver *driver) ReleaseBranchSessionLock(session *api.BranchSession) error {
	_, err := driver.engine.Exec(fmt.Sprintf(DeleteRowKeyByBranchID, driver.lockTable), session.BranchID)
	return err
}

func (driver *driver) ReleaseLockAndRemoveBranchSession(xid string, resourceID string, lockKeys []string) error {
	lockKeyCond := fmt.Sprintf(" and lock_key in %s", misc.MysqlAppendInParam(len(lockKeys)))
	deleteBranchArgs := []interface{}{xid, resourceID}
	for _, lockKey := range lockKeys {
		deleteBranchArgs = append(deleteBranchArgs, lockKey)
	}

	rowKeys := make([]interface{}, 0)
	for _, lockKey := range lockKeys {
		locks := schema.CollectRowLocks(lockKey, resourceID, xid, 0)
		for _, lockDO := range locks {
			rowKeys = append(rowKeys, lockDO.RowKey)
		}
	}
	whereCond := fmt.Sprintf("xid = ? and row_key in %s", misc.MysqlAppendInParam(len(rowKeys)))
	deleteRowKeyArgs := []interface{}{xid}
	deleteRowKeyArgs = append(deleteRowKeyArgs, rowKeys...)

	tx, err := driver.engine.DB().Begin()
	if err != nil {
		return err
	}
	_, err = tx.Exec(fmt.Sprintf(DeleteBranchTransactionByResourceID+lockKeyCond, driver.branchTable), deleteBranchArgs...)
	if err != nil {
		if rErr := tx.Rollback(); rErr != nil {
			log.Error(rErr)
		}
		return err
	}
	_, err = tx.Exec(fmt.Sprintf(DeleteRowKey, driver.lockTable, whereCond), deleteRowKeyArgs...)
	if err != nil {
		if rErr := tx.Rollback(); rErr != nil {
			log.Error(rErr)
		}
		return err
	}
	return tx.Commit()
}

// UpdateBranchSessionStatus update branch session status.
func (driver *driver) UpdateBranchSessionStatus(session *api.BranchSession, status api.BranchSession_BranchStatus) error {
	_, err := driver.engine.Exec(fmt.Sprintf(UpdateBranchTransaction, driver.branchTable),
		status,
		session.XID,
		session.BranchID)
	return err
}

// RemoveBranchSession remove branch session.
func (driver *driver) RemoveBranchSession(globalSession *api.GlobalSession, session *api.BranchSession) error {
	if session.Type == api.TCC {
		_, err := driver.engine.Exec(fmt.Sprintf(DeleteBranchTransaction, driver.branchTable),
			session.XID, session.BranchID)
		return err
	}
	if session.Type == api.AT {
		_, err := driver.engine.Exec(fmt.Sprintf(DeleteBranchTransactionByResourceID, driver.branchTable),
			session.XID, session.ResourceID)
		return err
	}
	return nil
}

// IsLockable Is lockable boolean.
func (driver *driver) IsLockable(xid string, resourceID string, lockKey string) bool {
	locks := schema.CollectRowLocks(lockKey, resourceID, xid, 0)
	var existedRowLocks []*schema.RowLock
	rowKeys := make([]interface{}, 0)
	for _, lockDO := range locks {
		rowKeys = append(rowKeys, lockDO.RowKey)
	}
	whereCond := fmt.Sprintf("row_key in %s", misc.MysqlAppendInParam(len(rowKeys)))

	err := driver.engine.SQL(fmt.Sprintf(QueryRowKey, driver.lockTable, whereCond), rowKeys...).Find(&existedRowLocks)
	if err != nil {
		log.Errorf(err.Error())
	}
	currentXID := locks[0].XID
	for _, rowLock := range existedRowLocks {
		if rowLock.XID != currentXID {
			return false
		}
	}
	return true
}

func distinctByKey(locks []*schema.RowLock) ([]*schema.RowLock, []interface{}) {
	result := make([]*schema.RowLock, 0)
	rowKeys := make([]interface{}, 0)
	lockMap := make(map[string]byte)
	for _, rowLock := range locks {
		l := len(lockMap)
		lockMap[rowLock.RowKey] = 0
		if len(lockMap) != l {
			result = append(result, rowLock)
			rowKeys = append(rowKeys, rowLock.RowKey)
		}
	}
	return result, rowKeys
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
