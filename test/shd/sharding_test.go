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
	driverName                            = "mysql"
	dataSourceName                        = "dksl:123456@tcp(127.0.0.1:13306)/drug?timeout=10s&readTimeout=10s&writeTimeout=10s&parseTime=true&loc=Local&charset=utf8mb4,utf8"
	selectDrugResource                    = "select id, drug_res_type_id, base_type, sale_price from drug_resource where id between ? and ?"
	selectDrugResourceLimit               = "select id, drug_res_type_id, base_type, sale_price from drug_resource where id between ? and ? limit ?,?"
	selectDrugResourceOrderBy1            = "select id, drug_res_type_id, base_type, sale_price from drug_resource where id between ? and ? order by id desc"
	selectDrugResourceOrderBy2            = "select id, drug_res_type_id, manufacturer_id, sale_price from drug_resource where id between ? and ? order by manufacturer_id desc, id asc"
	selectDrugResourceOrderByIDDescLimit  = "select id, drug_res_type_id, base_type, sale_price from drug_resource where id between ? and ? order by id desc limit ?, ?"
	selectDrugResourceOrderByIDDescLimit2 = "select id, drug_res_type_id, base_type, sale_price from drug_resource where id between ? and ? order by id desc limit ?"
	selectCount                           = "select count(1) from drug_resource where manufacturer_id = ?"

	deleteDrugResource = "delete from drug_resource where id between ? and ?"
	insertDrugResource = "INSERT INTO `drug_resource`(`id`, `drug_res_type_id`, `base_type`, `status`, `type_id`, " +
		"`dict_dosage_id`, `code`, `pym`, `name`, `manufacturer_id`, `approval_no`, `med_type`, `admin_code`, " +
		"`pack_unit_id`, `min_unit_id`, `pack_quantity`, `dosage_unit_id`, `dosage_quantity`, `pack_spec`, `take_method_id`, " +
		"`take_quantity`, `take_unit_id`, `take_unit_name`, `take_frequency`, `common_unit_type`, `purchase_price`, " +
		"`sale_price`, `cost_category_id`, `invoice_item_id`, `expensive_flag`, `psy_flag_i`, `psy_flag_ii`, `otc_flag`, " +
		"`nar_flag`, `inventory_upper`, `inventory_lower`, `create_time`, `modify_time`, `del_flag`, `consumable_type`, `tips`)" +
		" VALUES (20, 'hclb_kqkcl', 2, 1, 'hclb_kqkcl', '', 'ZKW00000898', 'xczj', '吸潮纸尖', 'sccj_tjjfylqxyxgs', " +
		"'天津械备20160055号', 3, '', 'jjdw_h', '', 1, '', 0.00, '15#', '', 0.0000, '', 'ml', '', 1, 0.0000, 0.0000, " +
		"'fylb_clf', 'fpxm_clf', 1, 0, 0, 0, 0, 0, 0, NULL, '2018-06-05 09:53:10', 0, 0, NULL);"
	updateDrugResource = "update drug_resource set sale_price = sale_price + 5 where id between ? and ?"
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
		var (
			id            int64
			drugResTypeId string
			baseType      int
			salePrice     float32
		)
		for rows.Next() {
			err := rows.Scan(&id, &drugResTypeId, &baseType, &salePrice)
			suite.NoError(err)
			suite.T().Logf("id: %d, drug resource type id: %s, base type: %d, sale price: %v", id, drugResTypeId, baseType, salePrice)
		}
	}
}

func (suite *_ShardingSuite) TestSelectLimit() {
	rows, err := suite.db.Query(selectDrugResourceLimit, 200, 250, 20, 10)
	if suite.NoErrorf(err, "select row error: %v", err) {
		var (
			id            int64
			drugResTypeId string
			baseType      int
			salePrice     float32
		)
		for rows.Next() {
			err := rows.Scan(&id, &drugResTypeId, &baseType, &salePrice)
			suite.NoError(err)
			suite.T().Logf("id: %d, drug resource type id: %s, base type: %d, sale price: %v", id, drugResTypeId, baseType, salePrice)
		}
	}
}

func (suite *_ShardingSuite) TestSelectOrderBy() {
	rows, err := suite.db.Query(selectDrugResourceOrderBy1, 200, 210)
	if suite.NoErrorf(err, "select row error: %v", err) {
		var (
			id            int64
			drugResTypeId string
			baseType      int
			salePrice     float32
		)
		for rows.Next() {
			err := rows.Scan(&id, &drugResTypeId, &baseType, &salePrice)
			suite.NoError(err)
			suite.T().Logf("id: %d, drug resource type id: %s, base type: %d, sale price: %v", id, drugResTypeId, baseType, salePrice)
		}
	}
}

func (suite *_ShardingSuite) TestSelectOrderBy2() {
	rows, err := suite.db.Query(selectDrugResourceOrderBy2, 200, 250)
	if suite.NoErrorf(err, "select row error: %v", err) {
		var (
			id             int64
			drugResTypeId  string
			manufacturerId string
			salePrice      float32
		)
		for rows.Next() {
			err := rows.Scan(&id, &drugResTypeId, &manufacturerId, &salePrice)
			suite.NoError(err)
			suite.T().Logf("id: %d, drug resource type id: %s, manufacturer id: %s, sale price: %v", id, drugResTypeId, manufacturerId, salePrice)
		}
	}
}

func (suite *_ShardingSuite) TestSelectOrderByAndLimit() {
	rows, err := suite.db.Query(selectDrugResourceOrderByIDDescLimit, 200, 300, 10, 20)
	if suite.NoErrorf(err, "select row error: %v", err) {
		var (
			id            int64
			drugResTypeId string
			baseType      int
			salePrice     float32
		)
		for rows.Next() {
			err := rows.Scan(&id, &drugResTypeId, &baseType, &salePrice)
			suite.NoError(err)
			suite.T().Logf("id: %d, drug resource type id: %s, base type: %d, sale price: %v", id, drugResTypeId, baseType, salePrice)
		}
	}
}

func (suite *_ShardingSuite) TestSelectOrderByAndLimit2() {
	rows, err := suite.db.Query(selectDrugResourceOrderByIDDescLimit2, 200, 300, 10)
	if suite.NoErrorf(err, "select row error: %v", err) {
		var (
			id            int64
			drugResTypeId string
			baseType      int
			salePrice     float32
		)
		for rows.Next() {
			err := rows.Scan(&id, &drugResTypeId, &baseType, &salePrice)
			suite.NoError(err)
			suite.T().Logf("id: %d, drug resource type id: %s, base type: %d, sale price: %v", id, drugResTypeId, baseType, salePrice)
		}
	}
}

func (suite *_ShardingSuite) TestSelectCount() {
	rows, err := suite.db.Query(selectCount, "sccj_w")
	if suite.NoErrorf(err, "select row error: %v", err) {
		var (
			count int64
		)
		for rows.Next() {
			err := rows.Scan(&count)
			suite.NoError(err)
			suite.T().Logf("count: %d", count)
		}
	}
}

func (suite *_ShardingSuite) TestDeleteDrugResource() {
	result, err := suite.db.Exec(deleteDrugResource, 10, 20)
	suite.Assert().Nil(err)
	affectedRows, err := result.RowsAffected()
	suite.Assert().Nil(err)
	suite.Assert().Equal(int64(11), affectedRows)
	time.Sleep(10 * time.Second)
}

func (suite *_ShardingSuite) TestInsertDrugResource() {
	result, err := suite.db.Exec(insertDrugResource)
	suite.Assert().Nil(err)
	affectedRows, err := result.RowsAffected()
	suite.Assert().Nil(err)
	suite.Assert().Equal(int64(1), affectedRows)
}

func (suite *_ShardingSuite) TestUpdateDrugResource() {
	result, err := suite.db.Exec(updateDrugResource, 200, 210)
	suite.Assert().Nil(err)
	affectedRows, err := result.RowsAffected()
	suite.Assert().Nil(err)
	suite.Assert().Equal(int64(11), affectedRows)

	rows, err := suite.db.Query(selectDrugResourceOrderBy1, 200, 210)
	if suite.NoErrorf(err, "select row error: %v", err) {
		var (
			id            int64
			drugResTypeId string
			baseType      int
			salePrice     float32
		)
		for rows.Next() {
			err := rows.Scan(&id, &drugResTypeId, &baseType, &salePrice)
			suite.NoError(err)
			suite.T().Logf("id: %d, drug resource type id: %s, base type: %d, sale price: %v", id, drugResTypeId, baseType, salePrice)
		}
	}
}

func (suite *_ShardingSuite) TearDownSuite() {
}
