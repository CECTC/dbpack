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

	_ "github.com/go-sql-driver/mysql" // register mysql
	"github.com/stretchr/testify/suite"
)

const (
	driverName                           = "mysql"
	dataSourceName                       = "dksl:123456@tcp(127.0.0.1:13306)/employees?timeout=10s&readTimeout=10s&writeTimeout=10s&parseTime=true&loc=Local&charset=utf8mb4,utf8"
	selectDrugResource                   = "select id, drug_res_type_id, base_type from drug_resource where id between ? and ?"
	selectDrugResourceOrderByIDDesc      = "select id, drug_res_type_id, base_type from drug_resource where id between ? and ? order by id desc"
	selectDrugResourceOrderByIDDescLimit = "select id, drug_res_type_id, base_type from drug_resource where id between ? and ? order by id desc limit ?, ?"
)

type _ShardingSuite struct {
	suite.Suite
	db *sql.DB
}

func TestSharding(t *testing.T) {
	suite.Run(t, new(_ShardingSuite))
}

func (suite *_ShardingSuite) SetupSuite() {
	db, err := sql.Open(driverName, dataSourceName)
	if suite.NoErrorf(err, "connection master db error: %v", err) {
		suite.db = db
	}
}

func (suite *_ShardingSuite) TestSelect() {
	rows, err := suite.db.Query(selectDrugResource, 200, 210)
	if suite.NoErrorf(err, "select row error: %v", err) {
		var id int64
		var drugResTypeId string
		var baseType int
		for rows.Next() {
			err := rows.Scan(&id, &drugResTypeId, &baseType)
			suite.NoError(err)
			suite.T().Logf("id: %d, drug resource type id: %s, base type: %d", id, drugResTypeId, baseType)
		}
	}
}

func (suite *_ShardingSuite) TestSelectOrderBy() {
	rows, err := suite.db.Query(selectDrugResourceOrderByIDDesc, 200, 210)
	if suite.NoErrorf(err, "select row error: %v", err) {
		var id int64
		var drugResTypeId string
		var baseType int
		for rows.Next() {
			err := rows.Scan(&id, &drugResTypeId, &baseType)
			suite.NoError(err)
			suite.T().Logf("id: %d, drug resource type id: %s, base type: %d", id, drugResTypeId, baseType)
		}
	}
}

func (suite *_ShardingSuite) TestSelectOrderByAndLimit() {
	rows, err := suite.db.Query(selectDrugResourceOrderByIDDescLimit, 200, 300, 10, 20)
	if suite.NoErrorf(err, "select row error: %v", err) {
		var id int64
		var drugResTypeId string
		var baseType int
		for rows.Next() {
			err := rows.Scan(&id, &drugResTypeId, &baseType)
			suite.NoError(err)
			suite.T().Logf("id: %d, drug resource type id: %s, base type: %d", id, drugResTypeId, baseType)
		}
	}
}

func (suite *_ShardingSuite) TearDownSuite() {
}
