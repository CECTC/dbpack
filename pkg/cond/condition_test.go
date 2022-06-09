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

package cond

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cectc/dbpack/pkg/visitor"
	"github.com/cectc/dbpack/third_party/parser"
	"github.com/cectc/dbpack/third_party/parser/ast"
	"github.com/cectc/dbpack/third_party/parser/opcode"
)

func TestConditionAnd(t *testing.T) {
	testCases := []*struct {
		title             string
		conditionA        Condition
		conditionB        Condition
		expectedCondition Condition
	}{
		{
			title:      "a and b => (a and b)",
			conditionA: &KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
			conditionB: &KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
			expectedCondition: &ComplexCondition{
				Op: opcode.And,
				Conditions: []Condition{
					&KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
					&KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
				},
			},
		},
		{
			title:      "a and (b and c) => (a and b and c)",
			conditionA: &KeyCondition{Key: "age", Op: opcode.EQ, Value: int64(18)},
			conditionB: &ComplexCondition{
				Op: opcode.And,
				Conditions: []Condition{
					&KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
					&KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
				},
			},
			expectedCondition: &ComplexCondition{
				Op: opcode.And,
				Conditions: []Condition{
					&KeyCondition{Key: "age", Op: opcode.EQ, Value: int64(18)},
					&KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
					&KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
				},
			},
		},
		{
			title:      "a and (b or c) => (a and b) or (a and c)",
			conditionA: &KeyCondition{Key: "age", Op: opcode.EQ, Value: int64(18)},
			conditionB: &ComplexCondition{
				Op: opcode.Or,
				Conditions: []Condition{
					&KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
					&KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
				},
			},
			expectedCondition: &ComplexCondition{
				Op: opcode.Or,
				Conditions: []Condition{
					&ComplexCondition{
						Op: opcode.And,
						Conditions: []Condition{
							&KeyCondition{Key: "age", Op: opcode.EQ, Value: int64(18)},
							&KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
						},
					},
					&ComplexCondition{
						Op: opcode.And,
						Conditions: []Condition{
							&KeyCondition{Key: "age", Op: opcode.EQ, Value: int64(18)},
							&KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
						},
					},
				},
			},
		},
		{
			title: "(a and b) and c => (a and b and c)",
			conditionA: &ComplexCondition{
				Op: opcode.And,
				Conditions: []Condition{
					&KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
					&KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
				},
			},
			conditionB: &KeyCondition{Key: "age", Op: opcode.EQ, Value: int64(18)},
			expectedCondition: &ComplexCondition{
				Op: opcode.And,
				Conditions: []Condition{
					&KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
					&KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
					&KeyCondition{Key: "age", Op: opcode.EQ, Value: int64(18)},
				},
			},
		},
		{
			title: "(a or b) and c => (a or c) and (b or c)",
			conditionA: &ComplexCondition{
				Op: opcode.Or,
				Conditions: []Condition{
					&KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
					&KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
				},
			},
			conditionB: &KeyCondition{Key: "age", Op: opcode.EQ, Value: int64(18)},
			expectedCondition: &ComplexCondition{
				Op: opcode.Or,
				Conditions: []Condition{
					&ComplexCondition{
						Op: opcode.And,
						Conditions: []Condition{
							&KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
							&KeyCondition{Key: "age", Op: opcode.EQ, Value: int64(18)},
						},
					},
					&ComplexCondition{
						Op: opcode.And,
						Conditions: []Condition{
							&KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
							&KeyCondition{Key: "age", Op: opcode.EQ, Value: int64(18)},
						},
					},
				},
			},
		},
		{
			title: "(a and b) and (c and c) => (a and b and c and d)",
			conditionA: &ComplexCondition{
				Op: opcode.And,
				Conditions: []Condition{
					&KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
					&KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
				},
			},
			conditionB: &ComplexCondition{
				Op: opcode.And,
				Conditions: []Condition{
					&KeyCondition{Key: "age", Op: opcode.EQ, Value: int64(18)},
					&KeyCondition{Key: "name", Op: opcode.EQ, Value: "scott"},
				},
			},
			expectedCondition: &ComplexCondition{
				Op: opcode.And,
				Conditions: []Condition{
					&KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
					&KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
					&KeyCondition{Key: "age", Op: opcode.EQ, Value: int64(18)},
					&KeyCondition{Key: "name", Op: opcode.EQ, Value: "scott"},
				},
			},
		},
		{
			title: "(a and b) and (c or d) => (a and b and c) or (a and b and d)",
			conditionA: &ComplexCondition{
				Op: opcode.And,
				Conditions: []Condition{
					&KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
					&KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
				},
			},
			conditionB: &ComplexCondition{
				Op: opcode.Or,
				Conditions: []Condition{
					&KeyCondition{Key: "age", Op: opcode.EQ, Value: int64(18)},
					&KeyCondition{Key: "name", Op: opcode.EQ, Value: "scott"},
				},
			},
			expectedCondition: &ComplexCondition{
				Op: opcode.Or,
				Conditions: []Condition{
					&ComplexCondition{
						Op: opcode.And,
						Conditions: []Condition{
							&KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
							&KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
							&KeyCondition{Key: "age", Op: opcode.EQ, Value: int64(18)},
						},
					},
					&ComplexCondition{
						Op: opcode.And,
						Conditions: []Condition{
							&KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
							&KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
							&KeyCondition{Key: "name", Op: opcode.EQ, Value: "scott"},
						},
					},
				},
			},
		},
		{
			title: "(a or b) and (c and d) => (a and c and d) or (b and c and d)",
			conditionA: &ComplexCondition{
				Op: opcode.Or,
				Conditions: []Condition{
					&KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
					&KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
				},
			},
			conditionB: &ComplexCondition{
				Op: opcode.And,
				Conditions: []Condition{
					&KeyCondition{Key: "age", Op: opcode.EQ, Value: int64(18)},
					&KeyCondition{Key: "name", Op: opcode.EQ, Value: "scott"},
				},
			},
			expectedCondition: &ComplexCondition{
				Op: opcode.Or,
				Conditions: []Condition{
					&ComplexCondition{
						Op: opcode.And,
						Conditions: []Condition{
							&KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
							&KeyCondition{Key: "age", Op: opcode.EQ, Value: int64(18)},
							&KeyCondition{Key: "name", Op: opcode.EQ, Value: "scott"},
						},
					},
					&ComplexCondition{
						Op: opcode.And,
						Conditions: []Condition{
							&KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
							&KeyCondition{Key: "age", Op: opcode.EQ, Value: int64(18)},
							&KeyCondition{Key: "name", Op: opcode.EQ, Value: "scott"},
						},
					},
				},
			},
		},
		{
			title: "(a or b) and (c or d) => (a and c) or (a and d) or (b and c) or (b and d)",
			conditionA: &ComplexCondition{
				Op: opcode.Or,
				Conditions: []Condition{
					&KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
					&KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
				},
			},
			conditionB: &ComplexCondition{
				Op: opcode.Or,
				Conditions: []Condition{
					&KeyCondition{Key: "age", Op: opcode.EQ, Value: int64(18)},
					&KeyCondition{Key: "name", Op: opcode.EQ, Value: "scott"},
				},
			},
			expectedCondition: &ComplexCondition{
				Op: opcode.Or,
				Conditions: []Condition{
					&ComplexCondition{
						Op: opcode.And,
						Conditions: []Condition{
							&KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
							&KeyCondition{Key: "age", Op: opcode.EQ, Value: int64(18)},
						},
					},
					&ComplexCondition{
						Op: opcode.And,
						Conditions: []Condition{
							&KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
							&KeyCondition{Key: "name", Op: opcode.EQ, Value: "scott"},
						},
					},
					&ComplexCondition{
						Op: opcode.And,
						Conditions: []Condition{
							&KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
							&KeyCondition{Key: "age", Op: opcode.EQ, Value: int64(18)},
						},
					},
					&ComplexCondition{
						Op: opcode.And,
						Conditions: []Condition{
							&KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
							&KeyCondition{Key: "name", Op: opcode.EQ, Value: "scott"},
						},
					},
				},
			},
		},
	}
	for _, c := range testCases {
		t.Run(c.title, func(t *testing.T) {
			condition := c.conditionA.And(c.conditionB)
			assert.Equal(t, c.expectedCondition, condition)
		})
	}
}

func TestConditionOr(t *testing.T) {
	testCases := []*struct {
		title             string
		conditionA        Condition
		conditionB        Condition
		expectedCondition Condition
	}{
		{
			title:      "a or b => (a or b)",
			conditionA: &KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
			conditionB: &KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
			expectedCondition: &ComplexCondition{
				Op: opcode.Or,
				Conditions: []Condition{
					&KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
					&KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
				},
			},
		},
		{
			title:      "a or (b and c) => (a or (b and c))",
			conditionA: &KeyCondition{Key: "age", Op: opcode.EQ, Value: int64(18)},
			conditionB: &ComplexCondition{
				Op: opcode.And,
				Conditions: []Condition{
					&KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
					&KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
				},
			},
			expectedCondition: &ComplexCondition{
				Op: opcode.Or,
				Conditions: []Condition{
					&KeyCondition{Key: "age", Op: opcode.EQ, Value: int64(18)},
					&ComplexCondition{
						Op: opcode.And,
						Conditions: []Condition{
							&KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
							&KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
						},
					},
				},
			},
		},
		{
			title:      "a or (b or c) => (a or b or c)",
			conditionA: &KeyCondition{Key: "age", Op: opcode.EQ, Value: int64(18)},
			conditionB: &ComplexCondition{
				Op: opcode.Or,
				Conditions: []Condition{
					&KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
					&KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
				},
			},
			expectedCondition: &ComplexCondition{
				Op: opcode.Or,
				Conditions: []Condition{
					&KeyCondition{Key: "age", Op: opcode.EQ, Value: int64(18)},
					&KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
					&KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
				},
			},
		},
		{
			title: "(a and b) or c => ((a and b) or c)",
			conditionA: &ComplexCondition{
				Op: opcode.And,
				Conditions: []Condition{
					&KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
					&KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
				},
			},
			conditionB: &KeyCondition{Key: "age", Op: opcode.EQ, Value: int64(18)},
			expectedCondition: &ComplexCondition{
				Op: opcode.Or,
				Conditions: []Condition{
					&ComplexCondition{
						Op: opcode.And,
						Conditions: []Condition{
							&KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
							&KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
						},
					},
					&KeyCondition{Key: "age", Op: opcode.EQ, Value: int64(18)},
				},
			},
		},
		{
			title: "(a or b) or c => (a or b or c)",
			conditionA: &ComplexCondition{
				Op: opcode.Or,
				Conditions: []Condition{
					&KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
					&KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
				},
			},
			conditionB: &KeyCondition{Key: "age", Op: opcode.EQ, Value: int64(18)},
			expectedCondition: &ComplexCondition{
				Op: opcode.Or,
				Conditions: []Condition{
					&KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
					&KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
					&KeyCondition{Key: "age", Op: opcode.EQ, Value: int64(18)},
				},
			},
		},
		{
			title: "(a and b) or (c and d) => ((a and b) or (c and d))",
			conditionA: &ComplexCondition{
				Op: opcode.And,
				Conditions: []Condition{
					&KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
					&KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
				},
			},
			conditionB: &ComplexCondition{
				Op: opcode.And,
				Conditions: []Condition{
					&KeyCondition{Key: "age", Op: opcode.EQ, Value: int64(18)},
					&KeyCondition{Key: "name", Op: opcode.EQ, Value: "scott"},
				},
			},
			expectedCondition: &ComplexCondition{
				Op: opcode.Or,
				Conditions: []Condition{
					&ComplexCondition{
						Op: opcode.And,
						Conditions: []Condition{
							&KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
							&KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
						},
					},
					&ComplexCondition{
						Op: opcode.And,
						Conditions: []Condition{
							&KeyCondition{Key: "age", Op: opcode.EQ, Value: int64(18)},
							&KeyCondition{Key: "name", Op: opcode.EQ, Value: "scott"},
						},
					},
				},
			},
		},
		{
			title: "(a and b) or (c or d) => ((a and b) or c or d)",
			conditionA: &ComplexCondition{
				Op: opcode.And,
				Conditions: []Condition{
					&KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
					&KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
				},
			},
			conditionB: &ComplexCondition{
				Op: opcode.Or,
				Conditions: []Condition{
					&KeyCondition{Key: "age", Op: opcode.EQ, Value: int64(18)},
					&KeyCondition{Key: "name", Op: opcode.EQ, Value: "scott"},
				},
			},
			expectedCondition: &ComplexCondition{
				Op: opcode.Or,
				Conditions: []Condition{
					&ComplexCondition{
						Op: opcode.And,
						Conditions: []Condition{
							&KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
							&KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
						},
					},
					&KeyCondition{Key: "age", Op: opcode.EQ, Value: int64(18)},
					&KeyCondition{Key: "name", Op: opcode.EQ, Value: "scott"},
				},
			},
		},
		{
			title: "(a or b) or (c and d) => (a or b or (c and d))",
			conditionA: &ComplexCondition{
				Op: opcode.Or,
				Conditions: []Condition{
					&KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
					&KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
				},
			},
			conditionB: &ComplexCondition{
				Op: opcode.And,
				Conditions: []Condition{
					&KeyCondition{Key: "age", Op: opcode.EQ, Value: int64(18)},
					&KeyCondition{Key: "name", Op: opcode.EQ, Value: "scott"},
				},
			},
			expectedCondition: &ComplexCondition{
				Op: opcode.Or,
				Conditions: []Condition{
					&KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
					&KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
					&ComplexCondition{
						Op: opcode.And,
						Conditions: []Condition{
							&KeyCondition{Key: "age", Op: opcode.EQ, Value: int64(18)},
							&KeyCondition{Key: "name", Op: opcode.EQ, Value: "scott"},
						},
					},
				},
			},
		},
		{
			title: "(a or b) or (c or d) => (a or b or c or d)",
			conditionA: &ComplexCondition{
				Op: opcode.Or,
				Conditions: []Condition{
					&KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
					&KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
				},
			},
			conditionB: &ComplexCondition{
				Op: opcode.Or,
				Conditions: []Condition{
					&KeyCondition{Key: "age", Op: opcode.EQ, Value: int64(18)},
					&KeyCondition{Key: "name", Op: opcode.EQ, Value: "scott"},
				},
			},
			expectedCondition: &ComplexCondition{
				Op: opcode.Or,
				Conditions: []Condition{
					&KeyCondition{Key: "id", Op: opcode.GE, Value: int64(3)},
					&KeyCondition{Key: "id", Op: opcode.LE, Value: int64(10)},
					&KeyCondition{Key: "age", Op: opcode.EQ, Value: int64(18)},
					&KeyCondition{Key: "name", Op: opcode.EQ, Value: "scott"},
				},
			},
		},
	}
	for _, c := range testCases {
		t.Run(c.title, func(t *testing.T) {
			condition := c.conditionA.Or(c.conditionB)
			assert.Equal(t, c.expectedCondition, condition)
		})
	}
}

func TestParseCondition(t *testing.T) {
	testCases := []*struct {
		sql               string
		args              []interface{}
		expectedCondition Condition
	}{
		{
			sql:  "select * from student where uid in (?, ?)",
			args: []interface{}{5, 8},
			expectedCondition: &ComplexCondition{
				Op: opcode.Or,
				Conditions: []Condition{
					&KeyCondition{Key: "uid", Op: opcode.EQ, Value: 5},
					&KeyCondition{Key: "uid", Op: opcode.EQ, Value: 8},
				},
			},
		},
		{
			sql:  "select * from student where ? = uid or uid = ?",
			args: []interface{}{5, 8},
			expectedCondition: &ComplexCondition{
				Op: opcode.Or,
				Conditions: []Condition{
					&KeyCondition{Key: "uid", Op: opcode.EQ, Value: 5},
					&KeyCondition{Key: "uid", Op: opcode.EQ, Value: 8},
				},
			},
		},
		{
			sql:  "select * from student where uid = ? or uid = ?",
			args: []interface{}{5, 8},
			expectedCondition: &ComplexCondition{
				Op: opcode.Or,
				Conditions: []Condition{
					&KeyCondition{Key: "uid", Op: opcode.EQ, Value: 5},
					&KeyCondition{Key: "uid", Op: opcode.EQ, Value: 8},
				},
			},
		},
		{
			sql:  "select * from student where uid between ? and ?",
			args: []interface{}{5, 8},
			expectedCondition: &ComplexCondition{
				Op: opcode.And,
				Conditions: []Condition{
					&KeyCondition{Key: "uid", Op: opcode.GE, Value: 5},
					&KeyCondition{Key: "uid", Op: opcode.LE, Value: 8},
				},
			},
		},
		{
			sql:  "select * from student where uid = ? and age = ?",
			args: []interface{}{5, 18},
			expectedCondition: &ComplexCondition{
				Op: opcode.And,
				Conditions: []Condition{
					&KeyCondition{Key: "uid", Op: opcode.EQ, Value: 5},
					&KeyCondition{Key: "age", Op: opcode.EQ, Value: 18},
				},
			},
		},
		{
			sql:  "select * from student where uid = ? or age = ?",
			args: []interface{}{5, 18},
			expectedCondition: &ComplexCondition{
				Op: opcode.Or,
				Conditions: []Condition{
					&KeyCondition{Key: "uid", Op: opcode.EQ, Value: 5},
					&KeyCondition{Key: "age", Op: opcode.EQ, Value: 18},
				},
			},
		},
		{
			// uid = 5 or ((age = 18 and name = jane) or (age = 18 and gender = 1))
			sql:  "select * from student where uid = ? or (age = ? and (name = ? or gender = ?))",
			args: []interface{}{5, 18, "jane", 1},
			expectedCondition: &ComplexCondition{
				Op: opcode.Or,
				Conditions: []Condition{
					&KeyCondition{Key: "uid", Op: opcode.EQ, Value: 5},
					&ComplexCondition{
						Op: opcode.And,
						Conditions: []Condition{
							&KeyCondition{Key: "age", Op: opcode.EQ, Value: 18},
							&KeyCondition{Key: "name", Op: opcode.EQ, Value: "jane"},
						},
					},
					&ComplexCondition{
						Op: opcode.And,
						Conditions: []Condition{
							&KeyCondition{Key: "age", Op: opcode.EQ, Value: 18},
							&KeyCondition{Key: "gender", Op: opcode.EQ, Value: 1},
						},
					},
				},
			},
		},
		{
			sql:  "select * from student where uid = ? and (age = ? or name = ?)",
			args: []interface{}{5, 18, "jane"},
			expectedCondition: &ComplexCondition{
				Op: opcode.Or,
				Conditions: []Condition{
					&ComplexCondition{
						Op: opcode.And,
						Conditions: []Condition{
							&KeyCondition{Key: "uid", Op: opcode.EQ, Value: 5},
							&KeyCondition{Key: "age", Op: opcode.EQ, Value: 18},
						},
					},
					&ComplexCondition{
						Op: opcode.And,
						Conditions: []Condition{
							&KeyCondition{Key: "uid", Op: opcode.EQ, Value: 5},
							&KeyCondition{Key: "name", Op: opcode.EQ, Value: "jane"},
						},
					},
				},
			},
		},
	}

	p := parser.New()
	for _, c := range testCases {
		t.Run(c.sql, func(t *testing.T) {
			stmt, err := p.ParseOneStmt(c.sql, "", "")
			if assert.NoErrorf(t, err, "parse sql statement err: %s", err) {
				stmt.Accept(&visitor.ParamVisitor{})
				sel := stmt.(*ast.SelectStmt)
				condition, err := ParseCondition(sel.Where, c.args...)
				assert.Nil(t, err)
				assert.Equal(t, c.expectedCondition, condition)
			}
		})
	}
}
