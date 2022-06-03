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

package sdb

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/cectc/dbpack/pkg/config"
	"github.com/cectc/dbpack/pkg/dt"
	"github.com/cectc/dbpack/pkg/dt/api"
	"github.com/cectc/dbpack/pkg/dt/storage/etcd"
)

const (
	dataSourceName2 = "dksl:123456@tcp(127.0.0.1:13306)/employees?interpolateParams=true&timeout=10s&readTimeout=10s&writeTimeout=10s&parseTime=true&loc=Local&charset=utf8mb4,utf8"

	insertEmployeeForDT   = `INSERT INTO employees ( id, emp_no, birth_date, first_name, last_name, gender, hire_date ) VALUES (?, ?, ?, ?, ?, ?, ?)`
	insertDepartmentForDT = `INSERT INTO departments ( id, dept_no, dept_name ) VALUES (?, ?, ?)`
	insertDeptEmpForDT    = `INSERT INTO dept_emp ( id, emp_no, dept_no, from_date, to_date ) VALUES (?, ?, ?, ?, ?)`
	insertSalariesForDT   = `INSERT INTO salaries ( id, emp_no, salary, from_date, to_date ) VALUES (?, ?, ?, ?, ?)`

	deleteEmployeeForDT    = `DELETE FROM employees WHERE id = ?`
	deleteDepartmentForDT  = `DELETE FROM departments WHERE id = ?`
	deleteDeptEmpForDT     = `DELETE FROM dept_emp WHERE id = ?`
	deleteDeptManagerForDT = `DELETE FROM dept_manager WHERE id=?`
	deleteSalariesForDT    = `DELETE FROM salaries WHERE id = ?`
)

type _DistributedTransactionSuite struct {
	suite.Suite
	db  *sql.DB
	db2 *sql.DB
}

func TestDistributedTransaction(t *testing.T) {
	suite.Run(t, new(_DistributedTransactionSuite))
}

func (suite *_DistributedTransactionSuite) SetupSuite() {
	var conf = &config.DistributedTransaction{
		ApplicationID:                    "svc",
		RetryDeadThreshold:               130000,
		RollbackRetryTimeoutUnlockEnable: true,
		EtcdConfig: clientv3.Config{
			Endpoints: []string{"localhost:2379", "localhost:2378", "localhost:2377"},
		},
	}

	driver := etcd.NewEtcdStore(conf.EtcdConfig)

	dt.InitDistributedTransactionManager(conf, driver)

	db, err := sql.Open(driverName, dataSourceName)
	if suite.NoErrorf(err, "connection error: %v", err) {
		suite.db = db
		db.Exec(insertEmployeeForDT, 1, 100001, "1992-06-04", "jane", "watson", "F", "2013-06-01")
		db.Exec(insertDepartmentForDT, 1, 1001, "sales")
		db.Exec(insertDeptEmpForDT, 1, 100001, 1001, "2020-01-01", "2022-01-01")
		db.Exec(insertSalariesForDT, 1, 100001, 8000, "2020-01-01", "2025-01-01")
	}

	db2, err := sql.Open(driverName, dataSourceName2)
	if suite.NoErrorf(err, "connection error: %v", err) {
		suite.db2 = db2
		db2.Exec(insertDeptEmpForDT, 2, 100002, 1002, "2020-01-01", "2022-01-01")
		db2.Exec(insertSalariesForDT, 2, 100002, 8000, "2020-01-01", "2025-01-01")
	}
}

func (suite *_DistributedTransactionSuite) TestDistributedTransactionPrepareRequest() {
	transactionManager := dt.GetDistributedTransactionManager()
	xid, err := transactionManager.Begin(context.Background(), "test-distributed-transaction", 60000)
	if suite.NoErrorf(err, "begin global transaction error: %v", err) {
		tx, err := suite.db.Begin()
		if suite.NoErrorf(err, "begin tx error: %v", err) {
			insertSql := fmt.Sprintf(`INSERT /*+ XID('%s') */ INTO dept_manager ( id, emp_no, dept_no, from_date, to_date ) VALUES (?, ?, ?, ?, ?)`, xid)
			result, err := tx.Exec(insertSql, 1, 100002, 1002, "2022-01-01", "2024-01-01")
			if suite.NoErrorf(err, "insert row error: %v", err) {
				affected, err := result.RowsAffected()
				if suite.NoErrorf(err, "insert row error: %v", err) {
					suite.Equal(int64(1), affected)
				}
			}

			deleteSql := fmt.Sprintf(`DELETE /*+ XID('%s') */ FROM dept_emp WHERE emp_no = ? and dept_no = ?`, xid)
			result, err = tx.Exec(deleteSql, 100002, 1002)
			if suite.NoErrorf(err, "delete row error: %v", err) {
				affected, err := result.RowsAffected()
				if suite.NoErrorf(err, "delete row error: %v", err) {
					suite.Equal(int64(1), affected)
				}
			}

			updateSql := fmt.Sprintf(`UPDATE /*+ XID('%s') */ salaries SET salary = ? WHERE emp_no = ?`, xid)
			result, err = tx.Exec(updateSql, 20000, 100002)
			if suite.NoErrorf(err, "update row error: %v", err) {
				affected, err := result.RowsAffected()
				if suite.NoErrorf(err, "update row error: %v", err) {
					suite.Equal(int64(1), affected)
				}
			}

			err = tx.Commit()
			if suite.NoErrorf(err, "commit local transaction error: %v", err) {
				status, err := transactionManager.Rollback(context.Background(), xid)
				if suite.NoErrorf(err, "rollback global transaction err: %v", err) {
					suite.Equal(api.Rollbacking, status)
				}
				time.Sleep(5 * time.Second)

				checkDeptEmp := `SELECT 1 FROM dept_emp WHERE emp_no = ? and dept_no = ?`
				rows, err := suite.db.Query(checkDeptEmp, 100002, 1002)
				if suite.NoErrorf(err, "check dept_emp error: %v", err) {
					var exists int
					if rows.Next() {
						err := rows.Scan(&exists)
						suite.NoError(err)
					}
					suite.Equal(1, exists)
				}

				checkSalaries := `SELECT 1 FROM salaries WHERE emp_no = ? and salary = ?`
				rows, err = suite.db.Query(checkSalaries, 100002, 8000)
				if suite.NoErrorf(err, "check salaries error: %v", err) {
					var exists int
					if rows.Next() {
						err := rows.Scan(&exists)
						suite.NoError(err)
					}
					suite.Equal(1, exists)
				}
			}
		}
	}
}

func (suite *_DistributedTransactionSuite) TestDistributedTransactionQueryRequest() {
	transactionManager := dt.GetDistributedTransactionManager()
	xid, err := transactionManager.Begin(context.Background(), "test-distributed-transaction", 60000)
	if suite.NoErrorf(err, "begin global transaction error: %v", err) {
		tx, err := suite.db2.Begin()
		if suite.NoErrorf(err, "begin tx error: %v", err) {
			//insertSql := fmt.Sprintf(`INSERT /*+ XID('%s') */ INTO dept_manager ( id, emp_no, dept_no, from_date, to_date ) VALUES (?, ?, ?, ?, ?)`, xid)
			//result, err := tx.Exec(insertSql, 1, 100002, 1002, "2022-01-01", "2024-01-01")
			//if suite.NoErrorf(err, "insert row error: %v", err) {
			//	affected, err := result.RowsAffected()
			//	if suite.NoErrorf(err, "insert row error: %v", err) {
			//		suite.Equal(int64(1), affected)
			//	}
			//}
			//
			deleteSql := fmt.Sprintf(`DELETE /*+ XID('%s') */ FROM dept_emp WHERE emp_no = ? and dept_no = ?`, xid)
			result, err := tx.Exec(deleteSql, 100002, 1002)
			if suite.NoErrorf(err, "delete row error: %v", err) {
				affected, err := result.RowsAffected()
				if suite.NoErrorf(err, "delete row error: %v", err) {
					suite.Equal(int64(1), affected)
				}
			}

			updateSql := fmt.Sprintf(`UPDATE /*+ XID('%s') */ salaries SET salary = ? WHERE emp_no = ?`, xid)
			result, err = tx.Exec(updateSql, 20000, 100002)
			if suite.NoErrorf(err, "update row error: %v", err) {
				affected, err := result.RowsAffected()
				if suite.NoErrorf(err, "update row error: %v", err) {
					suite.Equal(int64(1), affected)
				}
			}

			err = tx.Commit()
			if suite.NoErrorf(err, "commit local transaction error: %v", err) {
				status, err := transactionManager.Rollback(context.Background(), xid)
				if suite.NoErrorf(err, "rollback global transaction err: %v", err) {
					suite.Equal(api.Rollbacking, status)
				}
				time.Sleep(5 * time.Second)

				checkDeptEmp := `SELECT 1 FROM dept_emp WHERE emp_no = ? and dept_no = ?`
				rows, err := suite.db.Query(checkDeptEmp, 100002, 1002)
				if suite.NoErrorf(err, "check dept_emp error: %v", err) {
					var exists int
					if rows.Next() {
						err := rows.Scan(&exists)
						suite.NoError(err)
					}
					suite.Equal(1, exists)
				}

				checkSalaries := `SELECT 1 FROM salaries WHERE emp_no = ? and salary = ?`
				rows, err = suite.db2.Query(checkSalaries, 100002, 8000)
				if suite.NoErrorf(err, "check salaries error: %v", err) {
					var exists int
					if rows.Next() {
						err := rows.Scan(&exists)
						suite.NoError(err)
					}
					suite.Equal(1, exists)
				}
			}
		}
	}
}

func (suite *_DistributedTransactionSuite) TearDownSuite() {
	suite.db.Exec(deleteEmployeeForDT, 1)
	suite.db.Exec(deleteDepartmentForDT, 1)
	suite.db.Exec(deleteDeptEmpForDT, 1)
	suite.db.Exec(deleteSalariesForDT, 1)
	suite.db.Exec(deleteDeptManagerForDT, 1)

	suite.db2.Exec(deleteSalariesForDT, 2)
}
