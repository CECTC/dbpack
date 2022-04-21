package sdb

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/cectc/dbpack/pkg/config"
	"github.com/cectc/dbpack/pkg/driver"
	"github.com/cectc/dbpack/pkg/dt"
	"github.com/cectc/dbpack/pkg/dt/api"
	"github.com/cectc/dbpack/pkg/dt/storage/factory"
	_ "github.com/cectc/dbpack/pkg/dt/storage/mysql"
	"github.com/cectc/dbpack/pkg/resource"
	"github.com/cectc/dbpack/third_party/pools"
)

const (
	phiEmployeeDSN = `root:123456@tcp(127.0.0.1:3306)/employees?timeout=10s&readTimeout=10s&writeTimeout=10s&parseTime=true&loc=Local&charset=utf8mb4,utf8`
	phiMetaDSN     = `root:123456@tcp(127.0.0.1:3306)/meta?timeout=10s&readTimeout=10s&writeTimeout=10s&parseTime=true&loc=Local&charset=utf8mb4,utf8`

	insertEmployeeForDT   = `INSERT INTO employees ( id, emp_no, birth_date, first_name, last_name, gender, hire_date ) VALUES (?, ?, ?, ?, ?, ?, ?)`
	insertDepartmentForDT = `INSERT INTO departments ( id, dept_no, dept_name ) VALUES (?, ?, ?)`
	insertDeptEmpForDT    = `INSERT INTO dept_emp ( id, emp_no, dept_no, from_date, to_date ) VALUES (?, ?, ?, ?, ?)`
	insertSalariesForDT   = `INSERT INTO salaries ( id, emp_no, salary, from_date, to_date ) VALUES (?, ?, ?, ?, ?)`

	deleteEmployeeForDT   = `DELETE FROM employees WHERE id = ?`
	deleteDepartmentForDT = `DELETE FROM departments WHERE id = ?`
	deleteDeptEmpForDT    = `DELETE FROM dept_emp WHERE id = ?`
	deleteSalariesForDT   = `DELETE FROM salaries WHERE id = ?`
)

type _DistributedTransactionSuite struct {
	suite.Suite
	db *sql.DB
}

func TestDistributedTransaction(t *testing.T) {
	suite.Run(t, new(_DistributedTransactionSuite))
}

func (suite *_DistributedTransactionSuite) SetupSuite() {
	resource.InitDBManager([]*config.DataSource{
		{
			Name:                     "employees",
			DSN:                      phiEmployeeDSN,
			Capacity:                 10,
			MaxCapacity:              20,
			IdleTimeout:              60 * time.Second,
			PingInterval:             20 * time.Second,
			PingTimesForChangeStatus: 3,
		},
	}, func(dbName, dsn string) pools.Factory {
		collector, err := driver.NewConnector(dbName, dsn)
		if err != nil {
			panic(err)
		}
		return collector.NewBackendConnection
	})

	d, err := factory.Create("mysql", map[string]interface{}{
		"dsn":                  phiMetaDSN,
		"global_table":         "global_table",
		"branch_table":         "branch_table",
		"lock_table":           "lock_table",
		"query_limit":          100,
		"max_open_connections": 100,
		"max_idle_connections": 20,
		"max_lifetime":         "4h",
	})
	if err != nil {
		log.Fatalf("failed to construct mysql driver: %v", err)
	}

	dt.InitDistributedTransactionManager(&config.DistributedTransaction{
		Port:                             8092,
		Addressing:                       "localhost:8092",
		RetryDeadThreshold:               130000,
		MaxCommitRetryTimeout:            -1,
		MaxRollbackRetryTimeout:          -1,
		RollbackRetryTimeoutUnlockEnable: true,
		AsyncCommittingRetryPeriod:       10 * time.Second,
		CommittingRetryPeriod:            10 * time.Second,
		RollingBackRetryPeriod:           1 * time.Second,
		TimeoutRetryPeriod:               1 * time.Second,
		ClientParameters: config.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             1 * time.Second,
			PermitWithoutStream: true,
		},
		Storage: nil,
	}, d)

	db, err := sql.Open(driverName, dataSourceName)
	if suite.NoErrorf(err, "connection error: %v", err) {
		suite.db = db
		db.Exec(insertEmployeeForDT, 1, 100002, "1992-06-04", "jane", "watson", "F", "2013-06-01")
		db.Exec(insertDepartmentForDT, 1, 1002, "sales")
		db.Exec(insertDeptEmpForDT, 1, 100002, 1002, "2020-01-01", "2022-01-01")
		db.Exec(insertSalariesForDT, 1, 100002, 8000, "2020-01-01", "2025-01-01")
	}
}

func (suite *_DistributedTransactionSuite) TestDistributedTransaction() {
	transactionManager := dt.GetDistributedTransactionManager()
	xid, err := transactionManager.BeginLocal(context.Background(), &api.GlobalBeginRequest{
		Addressing:      "localhost:8092",
		Timeout:         60000,
		TransactionName: "test-distributed-transaction",
	})
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
				status, err := transactionManager.RollbackLocal(context.Background(),
					&api.GlobalRollbackRequest{
						XID: xid,
					})
				if suite.NoErrorf(err, "rollback global transaction err: %v", err) {
					suite.Equal(api.RolledBack, status)
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

func (suite *_DistributedTransactionSuite) TearDownSuite() {
	suite.db.Exec(deleteEmployeeForDT, 1)
	suite.db.Exec(deleteDepartmentForDT, 1)
	suite.db.Exec(deleteDeptEmpForDT, 1)
	suite.db.Exec(deleteSalariesForDT, 1)
}
