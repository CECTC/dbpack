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

package optimize

import (
	"context"
	"fmt"
	"strings"

	"github.com/antonmedv/expr"
	"github.com/antonmedv/expr/vm"
	"github.com/pkg/errors"

	"github.com/cectc/dbpack/pkg/cond"
	"github.com/cectc/dbpack/pkg/config"
	"github.com/cectc/dbpack/pkg/constant"
	"github.com/cectc/dbpack/pkg/dt/schema"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/meta"
	"github.com/cectc/dbpack/pkg/misc"
	"github.com/cectc/dbpack/pkg/plan"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/pkg/resource"
	"github.com/cectc/dbpack/pkg/topo"
	"github.com/cectc/dbpack/third_party/parser/ast"
	"github.com/cectc/dbpack/third_party/parser/format"
	"github.com/cectc/dbpack/third_party/parser/model"
	"github.com/cectc/dbpack/third_party/parser/opcode"
	"github.com/cectc/dbpack/third_party/types"
	driver "github.com/cectc/dbpack/third_party/types/parser_driver"
)

func (o Optimizer) optimizeInsert(ctx context.Context, stmt *ast.InsertStmt, args []interface{}) (proto.Plan, error) {
	var (
		alg        cond.ShardingAlgorithm
		topology   *topo.Topology
		tableMeta  schema.TableMeta
		shadowRule *config.ShadowRule
		shadow     bool
		columns    []string
		exists     bool
		err        error
	)
	tableName := stmt.Table.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName).Name.String()

	if alg, exists = o.algorithms[tableName]; !exists {
		return nil, errors.New("sharding algorithm should not be nil")
	}
	topology = alg.Topology()

	if shadowRule, exists = o.shadowRules[tableName]; exists {
		if misc.HasShadowHint(stmt.TableHints) {
			shadow = true
		}
		if !shadow {
			for i, column := range stmt.Columns {
				if strings.EqualFold(column.Name.String(), shadowRule.Column) && shadowRule.Expr != "" {
					var (
						out     interface{}
						program *vm.Program
					)

					shadowExpr := fmt.Sprintf(shadowRule.Expr, shadowRule.Column)
					columnValue := getColumnValue(ctx, stmt, i, args)
					env := map[string]interface{}{
						shadowRule.Column: columnValue,
					}
					program, err = expr.Compile(shadowExpr, expr.Env(env), expr.AsBool())
					if err != nil {
						return nil, errors.Wrapf(err, "expr %s should return bool", shadowExpr)
					}
					out, err = expr.Run(program, env)
					if err != nil {
						return nil, err
					}
					shadow = out.(bool)
				}
			}
		}
	}

	for _, column := range stmt.Columns {
		columns = append(columns, column.Name.String())
	}

	for db, tables := range topology.DBs {
		sqlDB := resource.GetDBManager(o.appid).GetDB(db)
		tableMeta, err = meta.GetTableMetaCache().GetTableMeta(ctx, sqlDB, tables[0])
		if err != nil {
			continue
		} else {
			break
		}
	}

	pk := tableMeta.GetPKName()
	index := findColumnIndex(stmt, pk)
	var pkValue interface{}

	if index == -1 {
		pkValue, err = alg.NextID()
		if err != nil {
			return nil, fmt.Errorf("failed to automatically generate a primary key: %w", err)
		}
		columns = append(columns, pk)
		stmt.Columns = append(stmt.Columns, &ast.ColumnName{
			Name: model.CIStr{
				O: pk,
			},
		})
		commandType := proto.CommandType(ctx)
		switch commandType {
		case constant.ComQuery:
			stmt.Lists[0] = append(stmt.Lists[0], &driver.ValueExpr{
				Datum: types.NewDatum(pkValue),
			})
		case constant.ComStmtExecute:
			args = append(args, pkValue)
		default:
			log.Panicf("should never happen!")
		}
	} else {
		pkValue = getColumnValue(ctx, stmt, index, args)
	}

	cd := &cond.KeyCondition{
		Key:   pk,
		Op:    opcode.EQ,
		Value: pkValue,
	}

	shards, err := cd.Shard(alg)
	if err != nil {
		return nil, errors.Wrap(err, "compute shards failed")
	}
	fullScan, shardMap := shards.ParseTopology(topology)
	if fullScan && !alg.AllowFullScan() {
		return nil, errors.New("full scan not allowed")
	}

	if len(shardMap) == 1 {
		for k, v := range shardMap {
			executor, exists := o.dbGroupExecutors[k]
			if !exists {
				return nil, errors.Errorf("db group %s should not be nil", k)
			}

			tbl := v[0]
			if shadow {
				tbl = fmt.Sprintf("%s%s", shadowRule.ShadowTablePrefix, tbl)
			}
			return &plan.InsertPlan{
				Database: k,
				Table:    tbl,
				Columns:  columns,
				Stmt:     stmt,
				Args:     args,
				Executor: executor,
			}, nil
		}
	}
	return nil, errors.New("should never happen!")
}

func findColumnIndex(stmt *ast.InsertStmt, columnName string) int {
	for i, column := range stmt.Columns {
		if column.Name.String() == columnName {
			return i
		}
	}
	return -1
}

func getColumnValue(ctx context.Context, stmt *ast.InsertStmt, index int, args []interface{}) interface{} {
	var value interface{}
	commandType := proto.CommandType(ctx)
	switch commandType {
	case constant.ComQuery:
		var sb strings.Builder
		ctx := format.NewRestoreCtx(constant.DBPackRestoreFormat, &sb)
		if err := stmt.Lists[0][index].Restore(ctx); err != nil {
			log.Panic(err)
		}
		value = sb.String()
	case constant.ComStmtExecute:
		value = args[index]
	default:
		log.Panicf("should never happen!")
	}

	switch val := value.(type) {
	case string:
		return val
	case []byte:
		return fmt.Sprintf("%s", val)
	default:
		return val
	}
}
