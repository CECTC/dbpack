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

package plan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/cectc/dbpack/pkg/mysql"
	"github.com/cectc/dbpack/pkg/resource"
	"github.com/cectc/dbpack/testdata"
	"github.com/cectc/dbpack/third_party/parser/ast"
	"github.com/cectc/dbpack/third_party/parser/model"
)

type _MergeResultTestSuite struct {
	suite.Suite
	environment *testdata.ShardingTestEnvironment
}

func TestMergeResult(t *testing.T) {
	suite.Run(t, new(_MergeResultTestSuite))
}

func (suite *_MergeResultTestSuite) SetupSuite() {
	environment := testdata.NewShardingTestEnvironment(suite.T())
	environment.RegisterDBResource(suite.T())
	suite.environment = environment
}

func (suite *_MergeResultTestSuite) TestMergeResultWithOutOrderByAndLimit() {
	db1 := resource.GetDBManager("test").GetDB("world_0")
	sql1 := "SELECT * FROM (" +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_0` WHERE `id` BETWEEN ? AND ?) UNION ALL " +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_1` WHERE `id` BETWEEN ? AND ?) UNION ALL " +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_2` WHERE `id` BETWEEN ? AND ?) UNION ALL " +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_3` WHERE `id` BETWEEN ? AND ?) UNION ALL " +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_4` WHERE `id` BETWEEN ? AND ?)) t ORDER BY `id` ASC"
	result1, warn1, err1 := db1.ExecuteSqlDirectly(sql1, 200, 210, 200, 210, 200, 210, 200, 210, 200, 210)
	if err1 != nil {
		suite.FailNow("query world_0 failed, err: %s", err1)
	}
	resultWithErr1 := &ResultWithErr{
		Database: "world_0",
		Result:   result1,
		Warning:  warn1,
		Error:    err1,
	}

	db2 := resource.GetDBManager("test").GetDB("world_1")
	sql2 := "SELECT * FROM (" +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_5` WHERE `id` BETWEEN ? AND ?) UNION ALL " +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_6` WHERE `id` BETWEEN ? AND ?) UNION ALL " +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_7` WHERE `id` BETWEEN ? AND ?) UNION ALL " +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_8` WHERE `id` BETWEEN ? AND ?) UNION ALL " +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_9` WHERE `id` BETWEEN ? AND ?)) t ORDER BY `id` ASC"
	result2, warn2, err2 := db2.ExecuteSqlDirectly(sql2, 200, 210, 200, 210, 200, 210, 200, 210, 200, 210)
	if err2 != nil {
		suite.FailNow("query world_1 failed, err: %s", err2)
	}
	resultWithErr2 := &ResultWithErr{
		Database: "world_1",
		Result:   result2,
		Warning:  warn2,
		Error:    err2,
	}
	result, warn := mergeResultWithoutOrderByAndLimit(context.Background(), []*ResultWithErr{
		resultWithErr1,
		resultWithErr2,
	})
	assert.Equal(suite.T(), uint16(0), warn)
	assert.Equal(suite.T(), 11, len(result.Rows))
	for i, row := range result.Rows {
		suite.T().Logf("---------- row %d ----------", i)
		binaryRow := row.(*mysql.BinaryRow)
		for j, value := range binaryRow.Values {
			switch value.Val.(type) {
			case string, []byte:
				suite.T().Logf("%d: %s", j, value.Val)
			default:
				suite.T().Logf("%d: %v", j, value.Val)
			}
		}
	}
}

func (suite *_MergeResultTestSuite) TestMergeResultWithLimit() {
	db1 := resource.GetDBManager("test").GetDB("world_0")
	sql1 := "SELECT * FROM (" +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_0` WHERE `id` BETWEEN ? AND ? LIMIT 30) UNION ALL " +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_1` WHERE `id` BETWEEN ? AND ? LIMIT 30) UNION ALL " +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_2` WHERE `id` BETWEEN ? AND ? LIMIT 30) UNION ALL " +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_3` WHERE `id` BETWEEN ? AND ? LIMIT 30) UNION ALL " +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_4` WHERE `id` BETWEEN ? AND ? LIMIT 30)) t ORDER BY `id` ASC"
	result1, warn1, err1 := db1.ExecuteSqlDirectly(sql1, 200, 250, 200, 250, 200, 250, 200, 250, 200, 250)
	if err1 != nil {
		suite.FailNow("query world_0 failed, err: %s", err1)
	}
	resultWithErr1 := &ResultWithErr{
		Database: "world_0",
		Result:   result1,
		Warning:  warn1,
		Error:    err1,
	}

	db2 := resource.GetDBManager("test").GetDB("world_1")
	sql2 := "SELECT * FROM (" +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_5` WHERE `id` BETWEEN ? AND ? LIMIT 30) UNION ALL " +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_6` WHERE `id` BETWEEN ? AND ? LIMIT 30) UNION ALL " +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_7` WHERE `id` BETWEEN ? AND ? LIMIT 30) UNION ALL " +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_8` WHERE `id` BETWEEN ? AND ? LIMIT 30) UNION ALL " +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_9` WHERE `id` BETWEEN ? AND ? LIMIT 30)) t ORDER BY `id` ASC"
	result2, warn2, err2 := db2.ExecuteSqlDirectly(sql2, 200, 250, 200, 250, 200, 250, 200, 250, 200, 250)
	if err2 != nil {
		suite.FailNow("query world_1 failed, err: %s", err2)
	}
	resultWithErr2 := &ResultWithErr{
		Database: "world_1",
		Result:   result2,
		Warning:  warn2,
		Error:    err2,
	}
	result, warn := mergeResultWithLimit(context.Background(), []*ResultWithErr{
		resultWithErr1,
		resultWithErr2,
	}, &Limit{
		Offset: 20,
		Count:  10,
	})
	assert.Equal(suite.T(), uint16(0), warn)
	assert.Equal(suite.T(), 10, len(result.Rows))
	for i, row := range result.Rows {
		suite.T().Logf("---------- row %d ----------", i)
		binaryRow := row.(*mysql.BinaryRow)
		for j, value := range binaryRow.Values {
			switch value.Val.(type) {
			case string, []byte:
				suite.T().Logf("%d: %s", j, value.Val)
			default:
				suite.T().Logf("%d: %v", j, value.Val)
			}
		}
	}
}

func (suite *_MergeResultTestSuite) TestMergeResultWithOrderByAndLimit() {
	db1 := resource.GetDBManager("test").GetDB("world_0")
	sql1 := "SELECT * FROM (" +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_0` WHERE `id` BETWEEN ? AND ? ORDER BY `id` DESC LIMIT 30) UNION ALL " +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_1` WHERE `id` BETWEEN ? AND ? ORDER BY `id` DESC LIMIT 30) UNION ALL " +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_2` WHERE `id` BETWEEN ? AND ? ORDER BY `id` DESC LIMIT 30) UNION ALL " +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_3` WHERE `id` BETWEEN ? AND ? ORDER BY `id` DESC LIMIT 30) UNION ALL " +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_4` WHERE `id` BETWEEN ? AND ? ORDER BY `id` DESC LIMIT 30)) t ORDER BY `id` DESC"
	result1, warn1, err1 := db1.ExecuteSqlDirectly(sql1, 200, 300, 200, 300, 200, 300, 200, 300, 200, 300)
	if err1 != nil {
		suite.FailNow("query world_0 failed, err: %s", err1)
	}
	resultWithErr1 := &ResultWithErr{
		Database: "world_0",
		Result:   result1,
		Warning:  warn1,
		Error:    err1,
	}

	db2 := resource.GetDBManager("test").GetDB("world_1")
	sql2 := "SELECT * FROM (" +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_5` WHERE `id` BETWEEN ? AND ? ORDER BY `id` DESC LIMIT 30) UNION ALL " +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_6` WHERE `id` BETWEEN ? AND ? ORDER BY `id` DESC LIMIT 30) UNION ALL " +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_7` WHERE `id` BETWEEN ? AND ? ORDER BY `id` DESC LIMIT 30) UNION ALL " +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_8` WHERE `id` BETWEEN ? AND ? ORDER BY `id` DESC LIMIT 30) UNION ALL " +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_9` WHERE `id` BETWEEN ? AND ? ORDER BY `id` DESC LIMIT 30)) t ORDER BY `id` DESC"
	result2, warn2, err2 := db2.ExecuteSqlDirectly(sql2, 200, 300, 200, 300, 200, 300, 200, 300, 200, 300)
	if err2 != nil {
		suite.FailNow("query world_1 failed, err: %s", err2)
	}
	resultWithErr2 := &ResultWithErr{
		Database: "world_1",
		Result:   result2,
		Warning:  warn2,
		Error:    err2,
	}
	result, warn := mergeResultWithOrderByAndLimit(context.Background(), []*ResultWithErr{
		resultWithErr1,
		resultWithErr2,
	}, &ast.OrderByClause{
		Items: []*ast.ByItem{
			{
				Expr: &ast.ColumnNameExpr{
					Name: &ast.ColumnName{
						Name: model.CIStr{
							O: "id",
						},
					},
				},
				Desc: false,
			},
		},
		ForUnion: false,
	}, &Limit{
		Offset: 10,
		Count:  20,
	})
	assert.Equal(suite.T(), uint16(0), warn)
	assert.Equal(suite.T(), 20, len(result.Rows))
	for i, row := range result.Rows {
		suite.T().Logf("---------- row %d ----------", i)
		binaryRow := row.(*mysql.BinaryRow)
		for j, value := range binaryRow.Values {
			switch value.Val.(type) {
			case string, []byte:
				suite.T().Logf("%d: %s", j, value.Val)
			default:
				suite.T().Logf("%d: %v", j, value.Val)
			}
		}
	}
}

func (suite *_MergeResultTestSuite) TestMergeResultWithOrderBy() {
	db1 := resource.GetDBManager("test").GetDB("world_0")
	sql1 := "SELECT * FROM (" +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_0` WHERE `id` BETWEEN ? AND ? ORDER BY `district` DESC,`id`) UNION ALL " +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_1` WHERE `id` BETWEEN ? AND ? ORDER BY `district` DESC,`id`) UNION ALL " +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_2` WHERE `id` BETWEEN ? AND ? ORDER BY `district` DESC,`id`) UNION ALL " +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_3` WHERE `id` BETWEEN ? AND ? ORDER BY `district` DESC,`id`) UNION ALL " +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_4` WHERE `id` BETWEEN ? AND ? ORDER BY `district` DESC,`id`)) t ORDER BY `district` DESC,`id`"
	result1, warn1, err1 := db1.ExecuteSqlDirectly(sql1, 200, 250, 200, 250, 200, 250, 200, 250, 200, 250)
	if err1 != nil {
		suite.FailNow("query world_0 failed, err: %s", err1)
	}
	resultWithErr1 := &ResultWithErr{
		Database: "world_0",
		Result:   result1,
		Warning:  warn1,
		Error:    err1,
	}

	db2 := resource.GetDBManager("test").GetDB("world_1")
	sql2 := "SELECT * FROM (" +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_5` WHERE `id` BETWEEN ? AND ? ORDER BY `district` DESC,`id`) UNION ALL " +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_6` WHERE `id` BETWEEN ? AND ? ORDER BY `district` DESC,`id`) UNION ALL " +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_7` WHERE `id` BETWEEN ? AND ? ORDER BY `district` DESC,`id`) UNION ALL " +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_8` WHERE `id` BETWEEN ? AND ? ORDER BY `district` DESC,`id`) UNION ALL " +
		"(SELECT `id`,`name`,`country_code`,`district`,`population` FROM `city_9` WHERE `id` BETWEEN ? AND ? ORDER BY `district` DESC,`id`)) t ORDER BY `district` DESC,`id`"
	result2, warn2, err2 := db2.ExecuteSqlDirectly(sql2, 200, 250, 200, 250, 200, 250, 200, 250, 200, 250)
	if err2 != nil {
		suite.FailNow("query world_1 failed, err: %s", err2)
	}
	resultWithErr2 := &ResultWithErr{
		Database: "world_1",
		Result:   result2,
		Warning:  warn2,
		Error:    err2,
	}
	result, warn := mergeResultWithOrderBy(context.Background(), []*ResultWithErr{
		resultWithErr1,
		resultWithErr2,
	}, &ast.OrderByClause{
		Items: []*ast.ByItem{
			{
				Expr: &ast.ColumnNameExpr{
					Name: &ast.ColumnName{
						Name: model.CIStr{
							O: "`district`",
						},
					},
				},
				Desc: true,
			},
			{
				Expr: &ast.ColumnNameExpr{
					Name: &ast.ColumnName{
						Name: model.CIStr{
							O: "id",
						},
					},
				},
				Desc: false,
			},
		}})
	assert.Equal(suite.T(), uint16(0), warn)
	assert.Equal(suite.T(), 51, len(result.Rows))
	for i, row := range result.Rows {
		suite.T().Logf("---------- row %d ----------", i)
		binaryRow := row.(*mysql.BinaryRow)
		for j, value := range binaryRow.Values {
			switch value.Val.(type) {
			case string, []byte:
				suite.T().Logf("%d: %s", j, value.Val)
			default:
				suite.T().Logf("%d: %v", j, value.Val)
			}
		}
	}
}

func (suite *_MergeResultTestSuite) TearDownSuite() {
	suite.environment.Shutdown(suite.T())
}
