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
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"

	"github.com/cectc/dbpack/pkg/cond"
	"github.com/cectc/dbpack/pkg/constant"
	"github.com/cectc/dbpack/pkg/dt/schema"
	"github.com/cectc/dbpack/pkg/meta"
	"github.com/cectc/dbpack/pkg/plan"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/pkg/resource"
	"github.com/cectc/dbpack/pkg/topo"
	"github.com/cectc/dbpack/pkg/visitor"
	"github.com/cectc/dbpack/third_party/parser"
)

func TestOptimizeQueryOnSingleDB(t *testing.T) {
	o := mockOptimizer()
	sql := "select * from student where id = ?"
	args := []interface{}{1}
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Error(err)
		return
	}
	stmt.Accept(&visitor.ParamVisitor{})
	pl, err := o.Optimize(context.Background(), stmt, args...)
	assert.Equal(t, nil, err)
	queryPlan, ok := pl.(*plan.QueryOnSingleDBPlan)
	assert.Equal(t, true, ok)
	assert.Equal(t, 1, len(queryPlan.Tables))
	assert.Equal(t, "school_0", queryPlan.Database)
	assert.Equal(t, "student_1", queryPlan.Tables[0])
}

func TestOptimizeQueryOnMultiDB(t *testing.T) {
	o := mockOptimizer()
	sql := "select * from student where id in (?, ?)"
	args := []interface{}{1, 15}
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Error(err)
		return
	}
	stmt.Accept(&visitor.ParamVisitor{})
	pl, err := o.Optimize(context.Background(), stmt, args...)
	assert.Equal(t, nil, err)
	queryPlan, ok := pl.(*plan.QueryOnMultiDBPlan)
	assert.Equal(t, true, ok)
	assert.Equal(t, 1, len(queryPlan.Plans[0].Tables))
	assert.Equal(t, "school_0", queryPlan.Plans[0].Database)
	assert.Equal(t, "student_1", queryPlan.Plans[0].Tables[0])

	assert.Equal(t, 1, len(queryPlan.Plans[1].Tables))
	assert.Equal(t, "school_1", queryPlan.Plans[1].Database)
	assert.Equal(t, "student_15", queryPlan.Plans[1].Tables[0])
}

func TestOptimizeDeleteOnSingleDB(t *testing.T) {
	o := mockOptimizer()
	sql := "delete from student where id = ?"
	args := []interface{}{1}
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Error(err)
		return
	}
	stmt.Accept(&visitor.ParamVisitor{})
	pl, err := o.Optimize(context.Background(), stmt, args...)
	assert.Equal(t, nil, err)
	deletePlan, ok := pl.(*plan.DeleteOnSingleDBPlan)
	assert.Equal(t, true, ok)
	assert.Equal(t, 1, len(deletePlan.Tables))
	assert.Equal(t, "school_0", deletePlan.Database)
	assert.Equal(t, "student_1", deletePlan.Tables[0])
}

func TestOptimizeDeleteOnMultiDB(t *testing.T) {
	o := mockOptimizer()
	sql := "delete from student where id in (?, ?)"
	args := []interface{}{1, 15}
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Error(err)
		return
	}
	stmt.Accept(&visitor.ParamVisitor{})
	pl, err := o.Optimize(context.Background(), stmt, args...)
	assert.Equal(t, nil, err)
	deletePlan, ok := pl.(*plan.DeleteOnMultiDBPlan)
	assert.Equal(t, true, ok)
	assert.Equal(t, 1, len(deletePlan.Plans[0].Tables))
	assert.Contains(t, []string{"school_0", "school_1"}, deletePlan.Plans[0].Database)
	assert.Contains(t, []string{"student_1", "student_15"}, deletePlan.Plans[0].Tables[0])

	assert.Equal(t, 1, len(deletePlan.Plans[1].Tables))
	assert.Contains(t, []string{"school_0", "school_1"}, deletePlan.Plans[1].Database)
	assert.Contains(t, []string{"student_1", "student_15"}, deletePlan.Plans[1].Tables[0])
}

func TestOptimizeInsert(t *testing.T) {
	o := mockOptimizer()
	sql := "insert into student(id, name, age) values (?, ? ,?)"
	args := []interface{}{18, "scott", 20}
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Error(err)
		return
	}
	stmt.Accept(&visitor.ParamVisitor{})

	resource.SetDBManager(&resource.DBManager{})
	var cache *meta.MysqlTableMetaCache
	patches := gomonkey.ApplyMethodFunc(cache, "GetTableMeta", func(ctx context.Context, db proto.DB, tableName string) (schema.TableMeta, error) {
		return schema.TableMeta{
			SchemaName: "school",
			TableName:  "student",
			Columns:    []string{"id", "age"},
			AllColumns: map[string]schema.ColumnMeta{
				"id": {
					TableCat:        "def",
					TableSchemeName: "school",
					TableName:       "student",
					ColumnName:      "id",
					DataType:        -5,
					DataTypeName:    "bigint",
					ColumnSize:      0,
					DecimalDigits:   19,
					NumPrecRadix:    0,
					Nullable:        0,
					Remarks:         "",
					ColumnDef:       "",
					SqlDataType:     0,
					SqlDatetimeSub:  0,
					CharOctetLength: 0,
					OrdinalPosition: 1,
					IsNullable:      "NO",
					IsAutoIncrement: "auto_increment",
				},
				"age": {
					TableCat:        "def",
					TableSchemeName: "school",
					TableName:       "student",
					ColumnName:      "age",
					DataType:        0,
					DataTypeName:    "int",
					ColumnSize:      0,
					DecimalDigits:   10,
					NumPrecRadix:    0,
					Nullable:        0,
					Remarks:         "",
					ColumnDef:       "",
					SqlDataType:     0,
					SqlDatetimeSub:  0,
					CharOctetLength: 0,
					OrdinalPosition: 2,
					IsNullable:      "NO",
					IsAutoIncrement: "",
				},
			},
			AllIndexes: map[string]schema.IndexMeta{
				"id": {
					Values: []schema.ColumnMeta{
						{
							TableCat:        "def",
							TableSchemeName: "school",
							TableName:       "student",
							ColumnName:      "id",
							DataType:        -5,
							DataTypeName:    "bigint",
							ColumnSize:      0,
							DecimalDigits:   19,
							NumPrecRadix:    0,
							Nullable:        0,
							Remarks:         "",
							ColumnDef:       "",
							SqlDataType:     0,
							SqlDatetimeSub:  0,
							CharOctetLength: 0,
							OrdinalPosition: 1,
							IsNullable:      "NO",
							IsAutoIncrement: "auto_increment",
						},
					},
					NonUnique:       false,
					IndexQualifier:  "",
					IndexName:       "PRIMARY",
					ColumnName:      "id",
					Type:            0,
					IndexType:       schema.IndexTypePrimary,
					AscOrDesc:       "A",
					Cardinality:     1,
					OrdinalPosition: 1,
				},
			},
		}, nil
	})
	defer patches.Reset()

	ctx := proto.WithCommandType(context.Background(), constant.ComStmtExecute)
	pl, err := o.Optimize(ctx, stmt, args...)
	assert.Equal(t, nil, err)
	insertPlan, ok := pl.(*plan.InsertPlan)
	assert.Equal(t, true, ok)
	assert.Equal(t, "school_1", insertPlan.Database)
	assert.Equal(t, "student_18", insertPlan.Table)
}

func mockOptimizer() *Optimizer {
	tp := mockTopology()
	topologies := map[string]*topo.Topology{
		"student": tp,
	}
	algorithms := map[string]cond.ShardingAlgorithm{
		"student": cond.NewNumberMod("id", false, tp),
	}

	o := &Optimizer{
		dbGroupExecutors: mockDBGroupExecutors(),
		algorithms:       algorithms,
		topologies:       topologies,
	}
	return o
}

func mockDBGroupExecutors() map[string]proto.DBGroupExecutor {
	return map[string]proto.DBGroupExecutor{
		"school_0": nil,
		"school_1": nil,
		"school_2": nil,
		"school_3": nil,
		"school_4": nil,
		"school_5": nil,
		"school_6": nil,
		"school_7": nil,
		"school_8": nil,
		"school_9": nil,
	}
}

func mockTopology() *topo.Topology {
	tp, _ := topo.ParseTopology("school", "student", map[int]string{
		0: "0-9",
		1: "10-19",
		2: "20-29",
		3: "30-39",
		4: "40-49",
		5: "50-59",
		6: "60-69",
		7: "70-79",
		8: "80-89",
		9: "90-99",
	})
	return tp
}
