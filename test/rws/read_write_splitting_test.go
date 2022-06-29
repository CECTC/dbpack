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

package rws

import (
	"database/sql"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql" // register mysql
	"github.com/stretchr/testify/suite"
)

const (
	driverName           = "mysql"
	dataSourceName       = "dksl:123456@tcp(127.0.0.1:13306)/employees?timeout=10s&readTimeout=10s&writeTimeout=10s&parseTime=true&loc=Local&charset=utf8mb4,utf8"
	masterDataSourceName = "root:123456@tcp(127.0.0.1:3306)/employees?timeout=10s&readTimeout=10s&writeTimeout=10s&parseTime=true&loc=Local&charset=utf8mb4,utf8"
	slaveDataSourceName  = "root:123456@tcp(127.0.0.1:3307)/employees?timeout=10s&readTimeout=10s&writeTimeout=10s&parseTime=true&loc=Local&charset=utf8mb4,utf8"

	insertEmployee  = `INSERT INTO employees ( emp_no, birth_date, first_name, last_name, gender, hire_date ) VALUES (?, ?, ?, ?, ?, ?)`
	selectEmployee1 = `SELECT emp_no, birth_date, first_name, last_name, gender, hire_date FROM employees WHERE emp_no = ?`
	selectEmployee2 = `SELECT /*+ UseDB('employees-master') */ emp_no, birth_date, first_name, last_name, gender, hire_date FROM employees WHERE emp_no = ?`
	updateEmployee  = `UPDATE employees set last_name = ? where emp_no = ?`
	deleteEmployee  = `DELETE FROM employees WHERE emp_no = ?`
)

type _ReadWriteSplittingSuite struct {
	suite.Suite
	db       *sql.DB
	masterDB *sql.DB
}

func TestReadWriteSplitting(t *testing.T) {
	suite.Run(t, new(_ReadWriteSplittingSuite))
}

func (suite *_ReadWriteSplittingSuite) SetupSuite() {
	db, err := sql.Open(driverName, dataSourceName)
	if suite.NoErrorf(err, "connection master db error: %v", err) {
		suite.db = db
	}

	slaveDB, err := sql.Open(driverName, slaveDataSourceName)
	if suite.NoErrorf(err, "connection slave db error: %v", err) {
		result, err := slaveDB.Exec(insertEmployee, 100001, "1992-02-11", "slave", "lewis", "M", "2014-09-01")
		if suite.NoErrorf(err, "insert row to slave db error: %v", err) {
			affected, err := result.RowsAffected()
			if suite.NoErrorf(err, "insert row to slave db error: %v", err) {
				suite.Equal(int64(1), affected)
			}
		}
	}

	masterDB, err := sql.Open(driverName, masterDataSourceName)
	if suite.NoErrorf(err, "connection slave db error: %v", err) {
		suite.masterDB = masterDB
		result, err := masterDB.Exec(insertEmployee, 100000, "1992-02-11", "scott", "lewis", "M", "2014-09-01")
		if suite.NoErrorf(err, "insert row to master db error: %v", err) {
			affected, err := result.RowsAffected()
			if suite.NoErrorf(err, "insert row to master db error: %v", err) {
				suite.Equal(int64(1), affected)
			}
		}
	}
}

func (suite *_ReadWriteSplittingSuite) TestDelete() {
	result, err := suite.db.Exec(deleteEmployee, 100000)
	if suite.NoErrorf(err, "delete row error: %v", err) {
		affected, err := result.RowsAffected()
		if suite.NoErrorf(err, "delete row error: %v", err) {
			suite.Equal(int64(1), affected)
		}
	}

	rows, err := suite.masterDB.Query(`SELECT EXISTS (SELECT 1 FROM employees WHERE emp_no = ?)`, 100000)
	if suite.NoErrorf(err, "check employees error: %v", err) {
		var exists int
		if rows.Next() {
			err := rows.Scan(&exists)
			suite.NoError(err)
		}
		suite.Equal(0, exists)
	}
}

func (suite *_ReadWriteSplittingSuite) TestInsert() {
	result, err := suite.db.Exec(insertEmployee, 100001, "1992-02-11", "master", "lewis", "M", "2014-09-01")
	if suite.NoErrorf(err, "insert row error: %v", err) {
		affected, err := result.RowsAffected()
		if suite.NoErrorf(err, "insert row error: %v", err) {
			suite.Equal(int64(1), affected)
		}
	}
	rows, err := suite.masterDB.Query(selectEmployee1, 100001)
	if suite.NoErrorf(err, "select row error: %v", err) {
		var empNo string
		var birthDate time.Time
		var firstName string
		var lastName string
		var gender string
		var hireDate time.Time
		if rows.Next() {
			err := rows.Scan(&empNo, &birthDate, &firstName, &lastName, &gender, &hireDate)
			suite.NoError(err)
		}
		suite.Equal("master", firstName)
	}
}

func (suite *_ReadWriteSplittingSuite) TestSelect1() {
	rows, err := suite.db.Query(selectEmployee1, 100001)
	if suite.NoErrorf(err, "select row error: %v", err) {
		var empNo string
		var birthDate time.Time
		var firstName string
		var lastName string
		var gender string
		var hireDate time.Time
		if rows.Next() {
			err := rows.Scan(&empNo, &birthDate, &firstName, &lastName, &gender, &hireDate)
			suite.NoError(err)
		}
		suite.Equal("slave", firstName)
	}
}

func (suite *_ReadWriteSplittingSuite) TestSelect2() {
	rows, err := suite.db.Query(selectEmployee2, 100001)
	if suite.NoErrorf(err, "select row error: %v", err) {
		var empNo string
		var birthDate time.Time
		var firstName string
		var lastName string
		var gender string
		var hireDate time.Time
		if rows.Next() {
			err := rows.Scan(&empNo, &birthDate, &firstName, &lastName, &gender, &hireDate)
			suite.NoError(err)
		}
		suite.Equal("master", firstName)
	}
}

func (suite *_ReadWriteSplittingSuite) TestUpdate() {
	result, err := suite.db.Exec(updateEmployee, "louis", 100001)
	if suite.NoErrorf(err, "update row error: %v", err) {
		affected, err := result.RowsAffected()
		if suite.NoErrorf(err, "update row error: %v", err) {
			suite.Equal(int64(1), affected)
		}
	}
	rows, err := suite.masterDB.Query(selectEmployee1, 100001)
	if suite.NoErrorf(err, "select row error: %v", err) {
		var empNo string
		var birthDate time.Time
		var firstName string
		var lastName string
		var gender string
		var hireDate time.Time
		if rows.Next() {
			err := rows.Scan(&empNo, &birthDate, &firstName, &lastName, &gender, &hireDate)
			suite.NoError(err)
		}
		suite.Equal("louis", lastName)
	}
}

func (suite *_ReadWriteSplittingSuite) TearDownSuite() {
}
