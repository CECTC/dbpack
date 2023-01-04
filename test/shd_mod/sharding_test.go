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
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql" // register mysql
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

const (
	driverName                    = "mysql"
	dataSourceName                = "dksl:123456@tcp(127.0.0.1:13306)/world?timeout=10s&readTimeout=10s&writeTimeout=10s&parseTime=true&loc=Local&charset=utf8mb4,utf8"
	selectCity                    = "select id, name, country_code, district, population from city where id between ? and ?"
	selectCityLimit               = "select id, name, country_code, district, population from city where id between ? and ? limit ?,?"
	selectCityOrderBy1            = "select id, name, country_code, district, population from city where id between ? and ? order by id desc"
	selectCityOrderBy2            = "select id, name, country_code, district, population from city where id between ? and ? order by district desc, id asc"
	selectCityOrderByIDDescLimit  = "select id, name, country_code, district, population from city where id between ? and ? order by id desc limit ?, ?"
	selectCityOrderByIDDescLimit2 = "select id, name, country_code, district, population from city where id between ? and ? order by id desc limit ?"
	selectJoin1                   = "select city.id, city.name, city.country_code, city.district, city.population, " +
		"country.name as countryName from city left join country on country.code = city.country_code " +
		"where city.id between ? and ? order by city.id desc limit ?"
	selectJoin2 = "select ct.id, ct.name, ct.country_code, ct.district, ct.population, " +
		"c.name as countryName from city ct left join country c on c.code = ct.country_code " +
		"where ct.id between ? and ? order by ct.id desc limit ?"
	selectCount = "select count(1) from city where country_code = ?"

	deleteCity           = "delete from city where id between ? and ?"
	insertCityWithoutID  = "INSERT INTO city (`name`, `country_code`, `district`, `population`) VALUES ('´s-Hertogenbosch', 'NLD', 'Noord-Brabant', 129170);"
	insertCity           = "INSERT INTO city (`id`, `name`, `country_code`, `district`, `population`) VALUES (20, '´s-Hertogenbosch', 'NLD', 'Noord-Brabant', 129170);"
	updateCity           = "update city set population = population + 5 where id between ? and ?"
	insertCityShadow     = "INSERT INTO city (`id`, `name`, `country_code`, `district`, `population`) VALUES  (?,?,?,?,?)"
	insertCityHintShadow = "INSERT /*+ Shadow() */ INTO city (`id`, `name`, `country_code`, `district`, `population`) VALUES  (?,?,?,?,?)"
	selectCityShadow     = "select id, name, country_code, district, population from pt_city_0"
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
	rows, err := suite.db.Query(selectCity, 200, 210)
	if suite.NoErrorf(err, "select row error: %v", err) {
		var (
			id          int64
			name        string
			countryCode string
			district    string
			population  int
		)
		for rows.Next() {
			err := rows.Scan(&id, &name, &countryCode, &district, &population)
			suite.NoError(err)
			suite.T().Logf("id: %d, name: %s, country code: %s, district: %s, population: %d",
				id, name, countryCode, district, population)
		}
	}
}

func (suite *_ShardingSuite) TestSelectLimit() {
	rows, err := suite.db.Query(selectCityLimit, 200, 250, 20, 10)
	if suite.NoErrorf(err, "select row error: %v", err) {
		var (
			id          int64
			name        string
			countryCode string
			district    string
			population  int
		)
		for rows.Next() {
			err := rows.Scan(&id, &name, &countryCode, &district, &population)
			suite.NoError(err)
			suite.T().Logf("id: %d, name: %s, country code: %s, district: %s, population: %d",
				id, name, countryCode, district, population)
		}
	}
}

func (suite *_ShardingSuite) TestSelectOrderBy() {
	rows, err := suite.db.Query(selectCityOrderBy1, 200, 210)
	if suite.NoErrorf(err, "select row error: %v", err) {
		var (
			id          int64
			name        string
			countryCode string
			district    string
			population  int
		)
		for rows.Next() {
			err := rows.Scan(&id, &name, &countryCode, &district, &population)
			suite.NoError(err)
			suite.T().Logf("id: %d, name: %s, country code: %s, district: %s, population: %d",
				id, name, countryCode, district, population)
		}
	}
}

func (suite *_ShardingSuite) TestSelectOrderBy2() {
	rows, err := suite.db.Query(selectCityOrderBy2, 200, 250)
	if suite.NoErrorf(err, "select row error: %v", err) {
		var (
			id          int64
			name        string
			countryCode string
			district    string
			population  int
		)
		for rows.Next() {
			err := rows.Scan(&id, &name, &countryCode, &district, &population)
			suite.NoError(err)
			suite.T().Logf("id: %d, name: %s, country code: %s, district: %s, population: %d",
				id, name, countryCode, district, population)
		}
	}
}

func (suite *_ShardingSuite) TestSelectOrderByAndLimit() {
	rows, err := suite.db.Query(selectCityOrderByIDDescLimit, 200, 300, 10, 20)
	if suite.NoErrorf(err, "select row error: %v", err) {
		var (
			id          int64
			name        string
			countryCode string
			district    string
			population  int
		)
		for rows.Next() {
			err := rows.Scan(&id, &name, &countryCode, &district, &population)
			suite.NoError(err)
			suite.T().Logf("id: %d, name: %s, country code: %s, district: %s, population: %d",
				id, name, countryCode, district, population)
		}
	}
}

func (suite *_ShardingSuite) TestSelectOrderByAndLimit2() {
	rows, err := suite.db.Query(selectCityOrderByIDDescLimit2, 200, 300, 10)
	if suite.NoErrorf(err, "select row error: %v", err) {
		var (
			id          int64
			name        string
			countryCode string
			district    string
			population  int
		)
		for rows.Next() {
			err := rows.Scan(&id, &name, &countryCode, &district, &population)
			suite.NoError(err)
			suite.T().Logf("id: %d, name: %s, country code: %s, district: %s, population: %d",
				id, name, countryCode, district, population)
		}
	}
}

func (suite *_ShardingSuite) TestSelectJoin1() {
	rows, err := suite.db.Query(selectJoin1, 200, 300, 10)
	if suite.NoErrorf(err, "select row error: %v", err) {
		var (
			id          int64
			name        string
			countryCode string
			district    string
			population  int
			countryName string
		)
		for rows.Next() {
			err := rows.Scan(&id, &name, &countryCode, &district, &population, &countryName)
			suite.NoError(err)
			suite.T().Logf("id: %d, name: %s, country code: %s, district: %s, population: %d, country name: %s",
				id, name, countryCode, district, population, countryName)
		}
	}
}

func (suite *_ShardingSuite) TestSelectJoin2() {
	rows, err := suite.db.Query(selectJoin2, 200, 300, 10)
	if suite.NoErrorf(err, "select row error: %v", err) {
		var (
			id          int64
			name        string
			countryCode string
			district    string
			population  int
			countryName string
		)
		for rows.Next() {
			err := rows.Scan(&id, &name, &countryCode, &district, &population, &countryName)
			suite.NoError(err)
			suite.T().Logf("id: %d, name: %s, country code: %s, district: %s, population: %d, country name: %s",
				id, name, countryCode, district, population, countryName)
		}
	}
}

func (suite *_ShardingSuite) TestSelectCount() {
	rows, err := suite.db.Query(selectCount, "CHN")
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

func (suite *_ShardingSuite) TestShowDatabases() {
	rows, err := suite.db.Query("SHOW DATABASES")
	if suite.NoErrorf(err, "show databases error: %v", err) {
		var (
			database string
		)
		for rows.Next() {
			err := rows.Scan(&database)
			suite.NoError(err)
			suite.T().Logf("database: %s", database)
		}
	}
}

func (suite *_ShardingSuite) TestShowEngines() {
	rows, err := suite.db.Query("SHOW ENGINES")
	if suite.NoErrorf(err, "show engines error: %v", err) {
		var (
			engine, support, comment, transactions, xa, savepoints interface{}
		)
		for rows.Next() {
			err := rows.Scan(&engine, &support, &comment, &transactions, &xa, &savepoints)
			suite.NoError(err)
			suite.T().Logf("%s	%s	%s	%s	%s	%s", engine, support, comment, transactions, xa, savepoints)
		}
	}
}

func (suite *_ShardingSuite) TestShowCreateDatabase() {
	rows, err := suite.db.Query("SHOW CREATE DATABASE world;")
	if suite.NoErrorf(err, "show engines error: %v", err) {
		var (
			database, createDatabase string
		)
		for rows.Next() {
			err := rows.Scan(&database, &createDatabase)
			suite.NoError(err)
			suite.T().Logf("%s	%s", database, createDatabase)
		}
	}

	rows, err = suite.db.Query("SHOW CREATE SCHEMA world;")
	if suite.NoErrorf(err, "show engines error: %v", err) {
		var (
			database, createDatabase string
		)
		for rows.Next() {
			err := rows.Scan(&database, &createDatabase)
			suite.NoError(err)
			suite.T().Logf("%s	%s", database, createDatabase)
		}
	}
}

func (suite *_ShardingSuite) TestShowTableStatus() {
	rows, err := suite.db.Query("SHOW TABLE STATUS LIKE 'city'")
	if suite.NoErrorf(err, "show table status error: %v", err) {
		var (
			name, engine, version, rowFormat, rowNum, avgRowLength, dataLength, maxDataLength, indexLength, dataFree,
			autoIncrement, createTime, updateTime, checkTime, collation, checkSum, createOption, comment interface{}
		)
		for rows.Next() {
			err := rows.Scan(&name, &engine, &version, &rowFormat, &rowNum, &avgRowLength, &dataLength, &maxDataLength,
				&indexLength, &dataFree, &autoIncrement, &createTime, &updateTime, &checkTime, &collation, &checkSum,
				&createOption, &comment)
			suite.NoError(err)
			suite.T().Log("table city:")
			suite.T().Logf("%s	%s	%s	%s	%s	%s	%s	%s	%s	%s	%s	%s	%s	%s	%s	%s	%s	%s",
				name, engine, version, rowFormat, rowNum, avgRowLength, dataLength, maxDataLength, indexLength, dataFree,
				autoIncrement, createTime, updateTime, checkTime, collation, checkSum, createOption, comment)
		}
	}
	rows, err = suite.db.Query("SHOW TABLE STATUS LIKE 'countrylanguage'")
	if suite.NoErrorf(err, "show table status error: %v", err) {
		var (
			name, engine, version, rowFormat, rowNum, avgRowLength, dataLength, maxDataLength, indexLength, dataFree,
			autoIncrement, createTime, updateTime, checkTime, collation, checkSum, createOption, comment interface{}
		)
		for rows.Next() {
			err := rows.Scan(&name, &engine, &version, &rowFormat, &rowNum, &avgRowLength, &dataLength, &maxDataLength,
				&indexLength, &dataFree, &autoIncrement, &createTime, &updateTime, &checkTime, &collation, &checkSum,
				&createOption, &comment)
			suite.NoError(err)
			suite.T().Log("table countrylanguage:")
			suite.T().Logf("%s	%s	%s	%s	%s	%s	%s	%s	%s	%s	%s	%s	%s	%s	%s	%s	%s	%s",
				name, engine, version, rowFormat, rowNum, avgRowLength, dataLength, maxDataLength, indexLength, dataFree,
				autoIncrement, createTime, updateTime, checkTime, collation, checkSum, createOption, comment)
		}
	}
}

func (suite *_ShardingSuite) TestShowTables() {
	rows, err := suite.db.Query("SHOW TABLES")
	if suite.NoErrorf(err, "show tables error: %v", err) {
		var (
			table string
		)
		for rows.Next() {
			err := rows.Scan(&table)
			suite.NoError(err)
			suite.T().Logf("%s", table)
		}
	}
}

func (suite *_ShardingSuite) TestShowTableMeta() {
	rows, err := suite.db.Query("SHOW COLUMNS FROM city")
	if suite.NoErrorf(err, "show columns error: %v", err) {
		var (
			field, sqlType, null, key, defaultValue, extra interface{}
		)
		suite.T().Log("table city columns:")
		for rows.Next() {
			err := rows.Scan(&field, &sqlType, &null, &key, &defaultValue, &extra)
			suite.NoError(err)
			suite.T().Logf("%s	%s	%s	%s	%s	%s", field, sqlType, null, key, defaultValue, extra)
		}
	}
	rows, err = suite.db.Query("SHOW INDEX FROM city")
	if suite.NoErrorf(err, "show index error: %v", err) {
		var (
			table, non_unique, key_name, seq_in_index, column_name, collation, cardinality, sub_part, packed, null,
			index_type, comment, index_comment, visible, expression interface{}
		)
		suite.T().Log("table city index:")
		for rows.Next() {
			err := rows.Scan(&table, &non_unique, &key_name, &seq_in_index, &column_name, &collation, &cardinality,
				&sub_part, &packed, &null, &index_type, &comment, &index_comment, &visible, &expression)
			suite.NoError(err)
			suite.T().Logf("%s	%s	%s	%s	%s	%s %s	%s	%s	%s	%s	%s	%s	%s 	%s", table, non_unique,
				key_name, seq_in_index, column_name, collation, cardinality, sub_part, packed, null, index_type,
				comment, index_comment, visible, expression)
		}
	}
}

func (suite *_ShardingSuite) TestCreateIndexAndDropIndex() {
	_, err := suite.db.Query("CREATE INDEX idx_countrycode ON city(country_code)")
	assert.Nil(suite.T(), err)
	_, err = suite.db.Query("DROP INDEX idx_countrycode ON city")
	assert.Nil(suite.T(), err)
}

func (suite *_ShardingSuite) TestDeleteCity() {
	result, err := suite.db.Exec(deleteCity, 10, 20)
	suite.Assert().Nil(err)
	affectedRows, err := result.RowsAffected()
	suite.Assert().Nil(err)
	suite.Assert().Equal(int64(11), affectedRows)
	time.Sleep(10 * time.Second)
}

func (suite *_ShardingSuite) TestInsertCity() {
	result, err := suite.db.Exec(insertCity)
	suite.Assert().Nil(err)
	affectedRows, err := result.RowsAffected()
	suite.Assert().Nil(err)
	suite.Assert().Equal(int64(1), affectedRows)
}

func (suite *_ShardingSuite) TestInsertCityWithoutID() {
	for i := 0; i < 3; i++ {
		result, err := suite.db.Exec(insertCityWithoutID)
		suite.Assert().Nil(err)
		affectedRows, err := result.RowsAffected()
		suite.Assert().Nil(err)
		suite.Assert().Equal(int64(1), affectedRows)
	}
}

func (suite *_ShardingSuite) TestUpdateCity() {
	result, err := suite.db.Exec(updateCity, 200, 210)
	suite.Assert().Nil(err)
	affectedRows, err := result.RowsAffected()
	suite.Assert().Nil(err)
	suite.Assert().Equal(int64(11), affectedRows)

	suite.TestSelectOrderBy()
}

func (suite *_ShardingSuite) TestLocalTransaction_2_Commit() {
	suite.TestSelectOrderBy()
	tx, err := suite.db.Begin()
	suite.Assert().Nil(err)
	result, err := tx.Exec(updateCity, 200, 210)
	suite.Assert().Nil(err)
	affectedRows, err := result.RowsAffected()
	suite.Assert().Nil(err)
	suite.Assert().Equal(int64(11), affectedRows)

	result, err = tx.Exec(deleteCity, 30, 40)
	suite.Assert().Nil(err)
	affectedRows, err = result.RowsAffected()
	suite.Assert().Nil(err)
	suite.Assert().Equal(int64(11), affectedRows)
	err = tx.Commit()
	suite.Assert().Nil(err)
	suite.TestSelectOrderBy()
}

func (suite *_ShardingSuite) TestLocalTransaction_1_Rollback() {
	suite.TestSelectOrderBy()
	tx, err := suite.db.Begin()
	suite.Assert().Nil(err)
	result, err := tx.Exec(updateCity, 200, 210)
	suite.Assert().Nil(err)
	affectedRows, err := result.RowsAffected()
	suite.Assert().Nil(err)
	suite.Assert().Equal(int64(11), affectedRows)

	result, err = tx.Exec(deleteCity, 30, 40)
	suite.Assert().Nil(err)
	affectedRows, err = result.RowsAffected()
	suite.Assert().Nil(err)
	suite.Assert().Equal(int64(11), affectedRows)
	err = tx.Rollback()
	suite.Assert().Nil(err)
	suite.TestSelectOrderBy()
}

func (suite *_ShardingSuite) TestExprShadow() {
	for i := 0; i < 20; i++ {
		_, err := suite.db.Exec(insertCityShadow, i*10, fmt.Sprintf("city%d", i), "US", fmt.Sprintf("district%d", i), 100000)
		suite.Assert().Nil(err)
	}
	rows, err := suite.db.Query(selectCityShadow)
	if suite.NoErrorf(err, "select row error: %v", err) {
		var (
			id          int64
			name        string
			countryCode string
			district    string
			population  int
		)
		for rows.Next() {
			err := rows.Scan(&id, &name, &countryCode, &district, &population)
			suite.NoError(err)
			suite.T().Logf("id: %d, name: %s, country code: %s, district: %s, population: %d",
				id, name, countryCode, district, population)
		}
	}
}

func (suite *_ShardingSuite) TestHintShadow() {
	for i := 0; i < 20; i++ {
		_, err := suite.db.Exec(insertCityHintShadow, 1000+i*10, fmt.Sprintf("city%d", i), "US", fmt.Sprintf("district%d", i), 100000)
		suite.Assert().Nil(err)
	}
	rows, err := suite.db.Query(selectCityShadow)
	if suite.NoErrorf(err, "select row error: %v", err) {
		var (
			id          int64
			name        string
			countryCode string
			district    string
			population  int
		)
		for rows.Next() {
			err := rows.Scan(&id, &name, &countryCode, &district, &population)
			suite.NoError(err)
			suite.T().Logf("id: %d, name: %s, country code: %s, district: %s, population: %d",
				id, name, countryCode, district, population)
		}
	}
}

func (suite *_ShardingSuite) TearDownSuite() {
}
