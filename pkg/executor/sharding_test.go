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

package executor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"gopkg.in/yaml.v3"

	"github.com/cectc/dbpack/pkg/config"
	"github.com/cectc/dbpack/pkg/constant"
	"github.com/cectc/dbpack/pkg/mysql"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/pkg/visitor"
	"github.com/cectc/dbpack/testdata"
	"github.com/cectc/dbpack/third_party/parser"
)

var conf = `
        name: redirect
        mode: shd
        config:
          transaction_timeout: 60000
          db_groups:
            - name: world_0
              load_balance_algorithm: RandomWeight
              data_sources:
                - name: world_0
                  weight: r10w10
            - name: world_1
              load_balance_algorithm: RandomWeight
              data_sources:
                - name: world_1
                  weight: r10w10
          global_tables:
            - country
            - countrylanguage
          logic_tables:
            - db_name: world
              table_name: city
              allow_full_scan: true
              sharding_rule:
                column: id
                sharding_algorithm: NumberMod
              sharding_key_generator:
                type: snowflake
                worker: 123
              topology:
                "0": 0-4
                "1": 5-9`

type _ShardingExecutorTestSuite struct {
	suite.Suite
	environment *testdata.ShardingTestEnvironment
	executor    proto.Executor
}

func TestMergeResult(t *testing.T) {
	suite.Run(t, new(_ShardingExecutorTestSuite))
}

func (suite *_ShardingExecutorTestSuite) SetupSuite() {
	environment := testdata.NewShardingTestEnvironment(suite.T())
	environment.RegisterDBResource(suite.T())
	suite.environment = environment

	executorConfig := suite.unmarshalExecutorConfig()
	executorConfig.AppID = "test"
	shardingExecutor, err := NewShardingExecutor(executorConfig)
	if err != nil {
		suite.T().Fatal(err)
	}
	suite.executor = shardingExecutor
}

func (suite *_ShardingExecutorTestSuite) TestQueryGlobalTable() {
	sql := "select code, name, continent, region from country where code = 'CHN'"
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	assert.Nil(suite.T(), err)
	stmt.Accept(&visitor.ParamVisitor{})

	ctx := proto.WithVariableMap(context.Background())
	ctx = proto.WithConnectionID(ctx, 1)
	ctx = proto.WithCommandType(ctx, constant.ComQuery)
	ctx = proto.WithQueryStmt(ctx, stmt)

	result, warns, err := suite.executor.ExecutorComQuery(ctx, sql)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), uint16(0), warns)
	mysqlResult := result.(*mysql.Result)
	for i, row := range mysqlResult.Rows {
		suite.T().Logf("---------- row %d ----------", i)
		textRow := row.(*mysql.TextRow)
		for j, value := range textRow.Values {
			switch value.Val.(type) {
			case string, []byte:
				suite.T().Logf("%d: %s", j, value.Val)
			default:
				suite.T().Logf("%d: %v", j, value.Val)
			}
		}
	}
}

func (suite *_ShardingExecutorTestSuite) TestPrepareExecuteGlobalTable() {
	sql := "select code, name, continent, region from country where code = ?"
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	assert.Nil(suite.T(), err)
	stmt.Accept(&visitor.ParamVisitor{})

	protoStmt := &proto.Stmt{
		SqlText:     sql,
		ParamsCount: 1,
		BindVars: map[string]interface{}{
			"v1": "CHN",
		},
		StmtNode: stmt,
	}
	ctx := proto.WithVariableMap(context.Background())
	ctx = proto.WithConnectionID(ctx, 1)
	ctx = proto.WithCommandType(ctx, constant.ComStmtExecute)
	ctx = proto.WithPrepareStmt(ctx, protoStmt)

	result, warns, err := suite.executor.ExecutorComStmtExecute(ctx, protoStmt)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), uint16(0), warns)
	mysqlResult := result.(*mysql.Result)
	for i, row := range mysqlResult.Rows {
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

func (suite *_ShardingExecutorTestSuite) unmarshalExecutorConfig() *config.Executor {
	var executorConfig *config.Executor
	if err := yaml.Unmarshal([]byte(conf), &executorConfig); err != nil {
		suite.T().Fatal(err)
	}
	return executorConfig
}

func (suite *_ShardingExecutorTestSuite) TearDownSuite() {
	suite.environment.Shutdown(suite.T())
}
