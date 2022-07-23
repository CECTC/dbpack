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
	"database/sql"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql" // register mysql
	"github.com/stretchr/testify/suite"
)

const (
	driverName = "mysql"

	// user:password@tcp(127.0.0.1:3306)/dbName?
	dataSourceName = "dksl:123456@tcp(127.0.0.1:13306)/employees?interpolateParams=true&timeout=10s&readTimeout=10s&writeTimeout=10s&parseTime=true&loc=Local&charset=utf8mb4,utf8"
	insertEmployee = `INSERT INTO employees ( emp_no, birth_date, first_name, last_name, gender, hire_date ) VALUES (?, ?, ?, ?, ?, ?)`
	selectEmployee = `SELECT emp_no, birth_date, first_name, last_name, gender, hire_date FROM employees WHERE emp_no = ?`
	updateEmployee = `UPDATE employees SET last_name = ? WHERE emp_no = ?`
	deleteEmployee = `DELETE FROM employees WHERE emp_no = ?`

	insertDepartment = `INSERT INTO departments( id, dept_no, dept_name ) values (?, ?, ?)`
	updateDepartment = `UPDATE departments SET dept_name = ? WHERE id = ?`
	selectDepartment = `SELECT id, dept_name FROM departments WHERE id = ?`
)

type _CRUDSuite struct {
	suite.Suite
	db *sql.DB
}

func TestCRUD(t *testing.T) {
	suite.Run(t, new(_CRUDSuite))
}

func (suite *_CRUDSuite) SetupSuite() {
	db, err := sql.Open(driverName, dataSourceName)
	if suite.NoErrorf(err, "connection error: %v", err) {
		suite.db = db
	}

	result, err := suite.db.Exec(insertEmployee, 100000, "1992-01-07", "scott", "lewis", "M", "2014-09-01")
	if suite.NoErrorf(err, "insert row error: %v", err) {
		affected, err := result.RowsAffected()
		if suite.NoErrorf(err, "insert row error: %v", err) {
			suite.Equal(int64(1), affected)
		}
	}
}

func (suite *_CRUDSuite) TestDelete() {
	result, err := suite.db.Exec(deleteEmployee, 100000)
	if suite.NoErrorf(err, "delete row error: %v", err) {
		affected, err := result.RowsAffected()
		if suite.NoErrorf(err, "delete row error: %v", err) {
			suite.Equal(int64(1), affected)
		}
	}
}

func (suite *_CRUDSuite) TestInsert() {
	result, err := suite.db.Exec(insertEmployee, 100001, "1992-01-07", "scott", "lewis", "M", "2014-09-01")
	if suite.NoErrorf(err, "insert row error: %v", err) {
		affected, err := result.RowsAffected()
		if suite.NoErrorf(err, "insert row error: %v", err) {
			suite.Equal(int64(1), affected)
		}
	}
}

func (suite *_CRUDSuite) TestInsertEncryption() {
	result, err := suite.db.Exec(insertDepartment, 1, "1001", "sunset")
	if suite.NoErrorf(err, "insert row error: %v", err) {
		affected, err := result.RowsAffected()
		if suite.NoErrorf(err, "insert row error: %v", err) {
			suite.Equal(int64(1), affected)
		}
	}

	rows, err := suite.db.Query(selectDepartment, 1)
	if suite.NoErrorf(err, "select row error: %v", err) {
		var (
			id       int64
			deptName string
		)
		for rows.Next() {
			err := rows.Scan(&id, &deptName)
			suite.NoError(err)
			suite.T().Logf("id: %d, dept name: %s", id, deptName)
		}
	}
}

func (suite *_CRUDSuite) TestSelect() {
	rows, err := suite.db.Query(selectEmployee, 100001)
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
		suite.Equal("scott", firstName)
	}
}

func (suite *_CRUDSuite) TestUpdate() {
	result, err := suite.db.Exec(updateEmployee, "louis", 100001)
	if suite.NoErrorf(err, "update row error: %v", err) {
		affected, err := result.RowsAffected()
		if suite.NoErrorf(err, "update row error: %v", err) {
			suite.Equal(int64(1), affected)
		}
	}
}

func (suite *_CRUDSuite) TestUpdateEncryption() {
	result, err := suite.db.Exec(updateDepartment, "moonlight", 1)
	if suite.NoErrorf(err, "update department error: %v", err) {
		affected, err := result.RowsAffected()
		if suite.NoErrorf(err, "update department error: %v", err) {
			suite.Equal(int64(1), affected)
		}
	}

	rows, err := suite.db.Query(selectDepartment, 1)
	if suite.NoErrorf(err, "select row error: %v", err) {
		var (
			id       int64
			deptName string
		)
		for rows.Next() {
			err := rows.Scan(&id, &deptName)
			suite.NoError(err)
			suite.T().Logf("id: %d, dept name: %s", id, deptName)
		}
	}
}

func (suite *_CRUDSuite) TearDownSuite() {
	result, err := suite.db.Exec(deleteEmployee, 100001)
	if suite.NoErrorf(err, "delete row error: %v", err) {
		affected, err := result.RowsAffected()
		if suite.NoErrorf(err, "delete row error: %v", err) {
			suite.Equal(int64(1), affected)
		}
	}
}
