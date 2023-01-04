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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cectc/dbpack/pkg/cond"
	"github.com/cectc/dbpack/pkg/topo"
	"github.com/cectc/dbpack/pkg/visitor"
	"github.com/cectc/dbpack/third_party/parser"
	"github.com/cectc/dbpack/third_party/parser/ast"
)

func TestQueryOnSingleDBPlan(t *testing.T) {
	testCases := []struct {
		selectSql           string
		tables              []string
		pk                  string
		args                []interface{}
		expectedGenerateSql string
	}{
		{
			selectSql: "select * from student where id in (?,?)",
			tables:    []string{"student_1", "student_5"},
			pk:        "id",
			args:      []interface{}{1, 5},
			expectedGenerateSql: "SELECT * FROM ((SELECT * FROM `student_1` WHERE `id` IN (?,?)) UNION ALL (SELECT * " +
				"FROM `student_5` WHERE `id` IN (?,?))) t ORDER BY `t`.`id` ASC",
		},
		{
			selectSql: "select * from student where id in (?,?) order by id desc",
			tables:    []string{"student_1", "student_5"},
			pk:        "id",
			args:      []interface{}{1, 5},
			expectedGenerateSql: "SELECT * FROM ((SELECT * FROM `student_1` WHERE `id` IN (?,?) ORDER BY `id` DESC) " +
				"UNION ALL (SELECT * FROM `student_5` WHERE `id` IN (?,?) ORDER BY `id` DESC)) t ORDER BY `t`.`id` DESC",
		},
		{
			selectSql: "select * from student where id in (?,?) order by id desc limit ?, ?",
			tables:    []string{"student_1", "student_5"},
			pk:        "id",
			args:      []interface{}{1, 5, 1000, 20},
			expectedGenerateSql: "SELECT * FROM ((SELECT * FROM `student_1` WHERE `id` IN (?,?) ORDER BY `id` DESC " +
				"LIMIT 1020) UNION ALL (SELECT * FROM `student_5` WHERE `id` IN (?,?) ORDER BY `id` DESC LIMIT 1020)) t ORDER BY `t`.`id` DESC",
		},
		{
			selectSql: "select student.id, student.name, city.province from student left join city on city.name = " +
				"student.native_place where student.id in (?,?) order by student.id desc limit ?, ?",
			tables: []string{"student_1", "student_5"},
			pk:     "id",
			args:   []interface{}{1, 5, 1000, 20},
			expectedGenerateSql: "SELECT * FROM ((SELECT `student_1`.`id`,`student_1`.`name`,`city`.`province` FROM `student_1` " +
				"LEFT JOIN `city` ON `city`.`name`=`student_1`.`native_place` WHERE `student_1`.`id` IN (?,?) " +
				"ORDER BY `student_1`.`id` DESC LIMIT 1020) UNION ALL (SELECT `student_5`.`id`,`student_5`.`name`,`city`.`province` " +
				"FROM `student_5` LEFT JOIN `city` ON `city`.`name`=`student_5`.`native_place` WHERE `student_5`.`id` IN (?,?) " +
				"ORDER BY `student_5`.`id` DESC LIMIT 1020)) t ORDER BY `t`.`id` DESC",
		},
		{
			selectSql: "select s.id, s.name, city.province from student s left join city on city.name = " +
				"s.native_place where s.id in (?,?) order by s.id desc limit ?, ?",
			tables: []string{"student_1", "student_5"},
			pk:     "id",
			args:   []interface{}{1, 5, 1000, 20},
			expectedGenerateSql: "SELECT * FROM ((SELECT `s`.`id`,`s`.`name`,`city`.`province` FROM `student_1` AS `s` " +
				"LEFT JOIN `city` ON `city`.`name`=`s`.`native_place` WHERE `s`.`id` IN (?,?) ORDER BY `s`.`id` DESC LIMIT 1020) " +
				"UNION ALL (SELECT `s`.`id`,`s`.`name`,`city`.`province` FROM `student_5` AS `s` LEFT JOIN `city` " +
				"ON `city`.`name`=`s`.`native_place` WHERE `s`.`id` IN (?,?) ORDER BY `s`.`id` DESC LIMIT 1020)) t ORDER BY `t`.`id` DESC",
		},
		{
			selectSql: "select student.id, student.name, exam.grade from student left join exam on exam.student_id = " +
				"student.id where student.id in (?,?) order by student.id desc limit ?, ?",
			tables: []string{"student_1", "student_5"},
			pk:     "id",
			args:   []interface{}{1, 5, 1000, 20},
			expectedGenerateSql: "SELECT * FROM ((SELECT `student_1`.`id`,`student_1`.`name`,`exam_1`.`grade` FROM `student_1` " +
				"LEFT JOIN `exam_1` ON `exam_1`.`student_id`=`student_1`.`id` WHERE `student_1`.`id` IN (?,?) " +
				"ORDER BY `student_1`.`id` DESC LIMIT 1020) UNION ALL (SELECT `student_5`.`id`,`student_5`.`name`,`exam_5`.`grade` " +
				"FROM `student_5` LEFT JOIN `exam_5` ON `exam_5`.`student_id`=`student_5`.`id` WHERE `student_5`.`id` IN (?,?) " +
				"ORDER BY `student_5`.`id` DESC LIMIT 1020)) t ORDER BY `t`.`id` DESC",
		},
		{
			selectSql: "select s.id, s.name, e.grade from student s left join exam e on e.student_id = " +
				"s.id where s.id in (?,?) order by s.id desc limit ?, ?",
			tables: []string{"student_1", "student_5"},
			pk:     "id",
			args:   []interface{}{1, 5, 1000, 20},
			expectedGenerateSql: "SELECT * FROM ((SELECT `s`.`id`,`s`.`name`,`e`.`grade` FROM `student_1` AS `s` " +
				"LEFT JOIN `exam_1` AS `e` ON `e`.`student_id`=`s`.`id` WHERE `s`.`id` IN (?,?) ORDER BY `s`.`id` DESC LIMIT 1020) " +
				"UNION ALL (SELECT `s`.`id`,`s`.`name`,`e`.`grade` FROM `student_5` AS `s` LEFT JOIN `exam_5` AS `e` " +
				"ON `e`.`student_id`=`s`.`id` WHERE `s`.`id` IN (?,?) ORDER BY `s`.`id` DESC LIMIT 1020)) t ORDER BY `t`.`id` DESC",
		},
	}

	for _, c := range testCases {
		t.Run(c.selectSql, func(t *testing.T) {
			p := parser.New()
			stmt, err := p.ParseOneStmt(c.selectSql, "", "")
			if err != nil {
				t.Error(err)
				return
			}
			stmt.Accept(&visitor.ParamVisitor{})
			selectStmt := stmt.(*ast.SelectStmt)
			plan := &QueryOnSingleDBPlan{
				Database:   "school_0",
				Tables:     c.tables,
				PK:         c.pk,
				Stmt:       selectStmt,
				Args:       c.args,
				Algorithms: mockAlgorithms(),
				GlobalTables: map[string]bool{
					"city": true,
				},
				Executor: nil,
			}
			var (
				sb   strings.Builder
				args []interface{}
			)
			plan.castLimit()
			err = plan.generate(context.Background(), &sb, &args)
			assert.Nil(t, err)
			assert.Equal(t, c.expectedGenerateSql, sb.String())
		})
	}
}

func mockAlgorithms() map[string]cond.ShardingAlgorithm {
	result := make(map[string]cond.ShardingAlgorithm)
	tp1, tp2 := mockTopology()
	algo1 := cond.NewNumberMod("id", true, tp1, nil)
	result["student"] = algo1
	algo2 := cond.NewNumberMod("student_id", true, tp2, nil)
	result["exam"] = algo2
	return result
}

func mockTopology() (*topo.Topology, *topo.Topology) {
	tp1, _ := topo.ParseTopology("school", "student", map[int]string{
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

	tp2, _ := topo.ParseTopology("school", "exam", map[int]string{
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
	return tp1, tp2
}
