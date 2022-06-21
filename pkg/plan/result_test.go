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
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/uber-go/atomic"
	utilrand "k8s.io/apimachinery/pkg/util/rand"

	"github.com/cectc/dbpack/pkg/constant"
	"github.com/cectc/dbpack/pkg/mysql"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/third_party/parser/ast"
	"github.com/cectc/dbpack/third_party/parser/model"
)

var (
	begin    *atomic.Int32
	students []*student
)

type student struct {
	id   int
	name string
	age  int
}

func newStudent(id int, age int) *student {
	return &student{
		id:   id,
		name: utilrand.String(5),
		age:  age,
	}
}

func (s *student) string() string {
	return fmt.Sprintf("id:%v name:%s age:%d", s.id, s.name, s.age)
}

func TestMergeResultWithOutOrderByAndLimit(t *testing.T) {
	buildStudents1(10015)
	patch1 := buildRowsNextPatch(10015)
	defer patch1.Reset()
	merge := func(commandType int) (*mysql.MergeResult, uint16) {
		return mergeResultWithOutOrderByAndLimit(proto.WithCommandType(context.Background(), byte(commandType)),
			[]*ResultWithErr{
				{
					Result:  &mysql.Result{},
					Warning: 0,
					Error:   nil,
				},
				{
					Result:  &mysql.Result{},
					Warning: 0,
					Error:   nil,
				},
				{
					Result:  &mysql.Result{},
					Warning: 0,
					Error:   nil,
				},
			})
	}
	t.Log("Merge COM_QUERY result:")
	result, _ := merge(constant.ComQuery)
	for _, row := range result.Rows {
		t.Logf("%s", row.Data())
	}
	begin.Swap(10000)
	t.Log("Merge COM_STMT_EXECUTE result:")
	result, _ = merge(constant.ComStmtExecute)
	for _, row := range result.Rows {
		t.Logf("%s", row.Data())
	}
}

func TestMergeResultWithOrderByAndLimit(t *testing.T) {
	buildStudents3(10050)
	patch1 := buildRowsNextPatch(10050)
	defer patch1.Reset()
	patch2 := buildTextRowDecodePatch()
	defer patch2.Reset()
	patch3 := buildBinaryRowDecodePatch()
	defer patch3.Reset()
	merge := func(commandType int) (*mysql.MergeResult, uint16) {
		return mergeResultWithOrderByAndLimit(proto.WithCommandType(context.Background(), byte(commandType)),
			[]*ResultWithErr{
				{
					Result:  &mysql.Result{Fields: []*mysql.Field{{Name: "id"}, {Name: "name"}, {Name: "age"}}},
					Warning: 0,
					Error:   nil,
				},
				{
					Result:  &mysql.Result{Fields: []*mysql.Field{{Name: "id"}, {Name: "name"}, {Name: "age"}}},
					Warning: 0,
					Error:   nil,
				},
				{
					Result:  &mysql.Result{Fields: []*mysql.Field{{Name: "id"}, {Name: "name"}, {Name: "age"}}},
					Warning: 0,
					Error:   nil,
				},
			}, &ast.OrderByClause{
				Items: []*ast.ByItem{
					{
						Expr: &ast.ColumnNameExpr{
							Name: &ast.ColumnName{
								Name: model.CIStr{
									O: "age",
								},
							},
						},
						Desc: false,
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
				},
				ForUnion: false,
			}, &Limit{
				Offset: 30,
				Count:  20,
			})
	}
	t.Log("Merge sorted COM_QUERY result:")
	result, _ := merge(constant.ComQuery)
	for _, row := range result.Rows {
		t.Logf("%s", row.Data())
	}
	begin.Swap(10000)
	t.Log("Merge sorted COM_STMT_EXECUTE result:")
	result, _ = merge(constant.ComStmtExecute)
	for _, row := range result.Rows {
		t.Logf("%s", row.Data())
	}
}

func TestMergeResultWithOrderBy(t *testing.T) {
	buildStudents2(10015)
	patch1 := buildRowsNextPatch(10015)
	defer patch1.Reset()
	patch2 := buildTextRowDecodePatch()
	defer patch2.Reset()
	patch3 := buildBinaryRowDecodePatch()
	defer patch3.Reset()
	merge := func(commandType int) (*mysql.MergeResult, uint16) {
		return mergeResultWithOrderBy(proto.WithCommandType(context.Background(), byte(commandType)),
			[]*ResultWithErr{
				{
					Result:  &mysql.Result{Fields: []*mysql.Field{{Name: "id"}, {Name: "name"}, {Name: "age"}}},
					Warning: 0,
					Error:   nil,
				},
				{
					Result:  &mysql.Result{Fields: []*mysql.Field{{Name: "id"}, {Name: "name"}, {Name: "age"}}},
					Warning: 0,
					Error:   nil,
				},
				{
					Result:  &mysql.Result{Fields: []*mysql.Field{{Name: "id"}, {Name: "name"}, {Name: "age"}}},
					Warning: 0,
					Error:   nil,
				},
			}, &ast.OrderByClause{
				Items: []*ast.ByItem{
					{
						Expr: &ast.ColumnNameExpr{
							Name: &ast.ColumnName{
								Name: model.CIStr{
									O: "age",
								},
							},
						},
						Desc: false,
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
				},
				ForUnion: false,
			})
	}
	t.Log("Merge sorted COM_QUERY result:")
	result, _ := merge(constant.ComQuery)
	for _, row := range result.Rows {
		t.Logf("%s", row.Data())
	}
	begin.Swap(10000)
	t.Log("Merge sorted COM_STMT_EXECUTE result:")
	result, _ = merge(constant.ComStmtExecute)
	for _, row := range result.Rows {
		t.Logf("%s", row.Data())
	}
}

func buildStudents1(end int) {
	begin = atomic.NewInt32(10000)
	students = make([]*student, 0)
	ages := []int{18, 19, 20, 21, 22}
	for i := 10000; i < end; i++ {
		index := rand.Int31n(5)
		students = append(students, newStudent(i+1, ages[index]))
	}
}

func buildStudents2(end int) {
	begin = atomic.NewInt32(10000)
	students = make([]*student, 0)
	ages := []int{18, 19, 20, 21, 22}
	for i := 10000; i < end; i++ {
		index := (i % 1000) / 3
		students = append(students, newStudent(i+1, ages[index]))
	}
}

func buildStudents3(end int) {
	begin = atomic.NewInt32(10000)
	students = make([]*student, 0)
	ages := []int{16, 17, 18, 19, 20, 21, 22}
	for i := 10000; i < end; i++ {
		index := (i % 1000) / 8
		students = append(students, newStudent(i+1, ages[index]))
	}
}

func buildRowsNextPatch(end int32) *gomonkey.Patches {
	var rows *mysql.Rows
	return gomonkey.ApplyMethodFunc(rows, "Next", func() (*mysql.Row, error) {
		begin.Inc()
		current := begin.Load()
		if current > end {
			return nil, io.EOF
		}
		i := current % 10000
		return &mysql.Row{
			Content:   []byte(students[i-1].string()),
			ResultSet: nil,
		}, nil
	})
}

func buildTextRowDecodePatch() *gomonkey.Patches {
	var row *mysql.TextRow
	return gomonkey.ApplyMethodFunc(row, "Decode", func() ([]*proto.Value, error) {
		current := begin.Load()
		i := current % 10000
		id := fmt.Sprintf("%v", students[i-1].id)
		name := students[i-1].name
		age := fmt.Sprintf("%v", students[i-1].age)
		return []*proto.Value{
			{
				Typ: constant.FieldTypeLong,
				Val: []byte(id),
			},
			{
				Typ: constant.FieldTypeVarChar,
				Val: []byte(name),
			},
			{
				Typ: constant.FieldTypeLong,
				Val: []byte(age),
			},
		}, nil
	})
}

func buildBinaryRowDecodePatch() *gomonkey.Patches {
	var row *mysql.BinaryRow
	return gomonkey.ApplyMethodFunc(row, "Decode", func() ([]*proto.Value, error) {
		current := begin.Load()
		i := current % 10000
		return []*proto.Value{
			{
				Typ: constant.FieldTypeLong,
				Val: int64(students[i-1].id),
			},
			{
				Typ: constant.FieldTypeVarChar,
				Val: []byte(students[i-1].name),
			},
			{
				Typ: constant.FieldTypeLong,
				Val: int64(students[i-1].age),
			},
		}, nil
	})
}
