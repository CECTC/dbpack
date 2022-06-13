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
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"github.com/cectc/dbpack/pkg/function"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/misc"
	"github.com/cectc/dbpack/third_party/parser/ast"
	"github.com/cectc/dbpack/third_party/parser/opcode"
	driver "github.com/cectc/dbpack/third_party/types/parser_driver"
)

type Condition interface {
	And(cond Condition) Condition
	Or(cond Condition) Condition
}

type KeyCondition struct {
	Key   string
	Op    opcode.Op
	Value interface{}
}

type ComplexCondition struct {
	Op         opcode.Op
	Conditions []Condition
}

type TrueCondition struct{}

type FalseCondition struct{}

func (cond *KeyCondition) And(cond2 Condition) Condition {
	switch c := cond2.(type) {
	case *KeyCondition:
		// a and b
		return &ComplexCondition{
			Op: opcode.And,
			Conditions: []Condition{
				cond,
				c,
			},
		}
	case *ComplexCondition:
		switch c.Op {
		case opcode.And:
			// a and (b and c) => a and b and c
			var result Condition = cond
			for _, cd := range c.Conditions {
				result = result.And(cd)
			}
			return result
		case opcode.Or:
			// a and (b or c) => (a and b) or (a and c)
			var result = &ComplexCondition{
				Op:         opcode.Or,
				Conditions: make([]Condition, 0),
			}
			for _, cd := range c.Conditions {
				result.Conditions = append(result.Conditions, cond.And(cd))
			}
			return result
		}
	}
	return nil
}

func (cond *KeyCondition) Or(cond2 Condition) Condition {
	switch c := cond2.(type) {
	case *KeyCondition:
		// a or b
		return &ComplexCondition{
			Op: opcode.Or,
			Conditions: []Condition{
				cond,
				c,
			},
		}
	case *ComplexCondition:
		switch c.Op {
		case opcode.And:
			// a or (b and c) => a or (b and c)
			var result = &ComplexCondition{
				Op:         opcode.Or,
				Conditions: []Condition{cond, c},
			}
			return result
		case opcode.Or:
			// a or (b or c) => a or b or c
			var result Condition = cond
			for _, cd := range c.Conditions {
				result = result.Or(cd)
			}
			return result
		}
	}
	return nil
}

func (cond *ComplexCondition) And(cond2 Condition) Condition {
	switch cond.Op {
	case opcode.And:
		switch c := cond2.(type) {
		case *KeyCondition:
			// (a and b) and c => a and b and c
			var result = &ComplexCondition{
				Op:         opcode.And,
				Conditions: make([]Condition, 0),
			}
			result.Conditions = append(result.Conditions, cond.Conditions...)
			result.Conditions = append(result.Conditions, cond2)
			return result
		case *ComplexCondition:
			switch c.Op {
			case opcode.And:
				// (a and b) and (c and d) => a and b and c and d
				for _, cd := range c.Conditions {
					cond.Conditions = append(cond.Conditions, cd)
				}
				return cond
			case opcode.Or:
				// (a and b) and (c or d) => (a and b and c) or (a and b and d)
				var result = &ComplexCondition{
					Op:         opcode.Or,
					Conditions: make([]Condition, 0),
				}
				for _, cd := range c.Conditions {
					result.Conditions = append(result.Conditions, cond.And(cd))
				}
				return result
			}
		}
	case opcode.Or:
		switch c := cond2.(type) {
		case *KeyCondition:
			// (a or b) and c => (a and c) or (b and c)
			var result = &ComplexCondition{
				Op:         opcode.Or,
				Conditions: make([]Condition, 0),
			}
			for _, cd := range cond.Conditions {
				result.Conditions = append(result.Conditions, cd.And(c))
			}
			return result
		case *ComplexCondition:
			switch c.Op {
			case opcode.And:
				// (a or b) and (c and d) => (a and c and d) or (b and c and d)
				var result = &ComplexCondition{
					Op:         opcode.Or,
					Conditions: make([]Condition, 0),
				}
				for _, cd := range cond.Conditions {
					result.Conditions = append(result.Conditions, cd.And(c))
				}
				return result
			case opcode.Or:
				// (a or b) and (c or d) => (a and c) or (a and d) or (b and c) or (b and d)
				var result = &ComplexCondition{
					Op:         opcode.Or,
					Conditions: make([]Condition, 0),
				}
				for _, cd1 := range cond.Conditions {
					for _, cd2 := range c.Conditions {
						result.Conditions = append(result.Conditions, cd1.And(cd2))
					}
				}
				return result
			}
		}
	}
	return nil
}

func (cond *ComplexCondition) Or(cond2 Condition) Condition {
	switch cond.Op {
	case opcode.And:
		switch c := cond2.(type) {
		case *KeyCondition:
			// (a and b) or c => (a and b) or c
			var result = &ComplexCondition{
				Op:         opcode.Or,
				Conditions: []Condition{cond, c},
			}
			return result
		case *ComplexCondition:
			switch c.Op {
			case opcode.And:
				// (a and b) or (c and d) => (a and b) or (c and d)
				return &ComplexCondition{
					Op:         opcode.Or,
					Conditions: []Condition{cond, c},
				}
			case opcode.Or:
				// (a and b) or (c or d) => (a and b) or c or d
				var result = &ComplexCondition{
					Op:         opcode.Or,
					Conditions: make([]Condition, 0),
				}
				result.Conditions = append(result.Conditions, cond)
				for _, cd := range c.Conditions {
					result.Conditions = append(result.Conditions, cd)
				}
				return result
			}
		}
	case opcode.Or:
		switch c := cond2.(type) {
		case *KeyCondition:
			// (a or b) or c => a or b or c
			var result = &ComplexCondition{
				Op:         opcode.Or,
				Conditions: make([]Condition, 0),
			}
			result.Conditions = append(result.Conditions, cond.Conditions...)
			result.Conditions = append(result.Conditions, cond2)
			return result
		case *ComplexCondition:
			switch c.Op {
			case opcode.And:
				// (a or b) or (c and d) => a or b or (c and d)
				var result = &ComplexCondition{
					Op:         opcode.Or,
					Conditions: make([]Condition, 0),
				}
				for _, cd := range cond.Conditions {
					result.Conditions = append(result.Conditions, cd)
				}
				result.Conditions = append(result.Conditions, c)
				return result
			case opcode.Or:
				// (a or b) or (c or d) => a or b or c or d
				for _, cd := range c.Conditions {
					cond.Conditions = append(cond.Conditions, cd)
				}
				return cond
			}
		}
	}
	return nil
}

func (cond TrueCondition) And(cond2 Condition) Condition {
	return cond2
}

func (cond TrueCondition) Or(cond2 Condition) Condition {
	return cond
}

func (cond FalseCondition) And(cond2 Condition) Condition {
	return cond
}

func (cond FalseCondition) Or(cond2 Condition) Condition {
	return cond2
}

type ConditionSlice []Condition

func (s ConditionSlice) Len() int { return len(s) }

func (s ConditionSlice) Less(i, j int) bool {
	switch cond1 := s[i].(type) {
	case *KeyCondition:
		switch cond2 := s[j].(type) {
		case *KeyCondition:
			s1 := fmt.Sprintf("%s%s%v", cond1.Key, cond1.Op.String(), cond1.Value)
			s2 := fmt.Sprintf("%s%s%v", cond2.Key, cond2.Op.String(), cond2.Value)
			if strings.Compare(s1, s2) == -1 {
				return true
			}
		}
	}
	return false
}

func (s ConditionSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func ParseCondition(cond ast.ExprNode, args ...interface{}) (Condition, error) {
	switch expr := cond.(type) {
	case *ast.BinaryOperationExpr:
		switch expr.Op {
		case opcode.EQ, opcode.NE, opcode.GT, opcode.LT, opcode.GE, opcode.LE:
			return ParseCompareExpression(expr, args...)
		case opcode.Plus, opcode.Minus, opcode.Mul, opcode.Div:
			return ParseMathCondition(expr, args...)
		case opcode.LogicAnd, opcode.LogicOr:
			return ParseLogicalCondition(expr, args...)
		default:
			return nil, errors.Errorf("unsupported binary operation expr opcode %T", expr.Op)
		}
	case *ast.PatternLikeExpr:
		return ParseLikeCondition(expr, args...)
	case *ast.BetweenExpr:
		return ParseBetweenCondition(expr, args...)
	case *ast.PatternInExpr:
		return ParseInCondition(expr, args...)
	case *ast.ParenthesesExpr:
		return ParseCondition(expr.Expr, args...)
	default:
		return ParseValCondition(expr, args...)
	}
}

func ParseCompareExpression(expr *ast.BinaryOperationExpr, args ...interface{}) (Condition, error) {
	var (
		key    *ast.ColumnNameExpr
		value1 interface{}
		value2 interface{}
		value  interface{}
		op     opcode.Op
		err1   error
		err2   error
	)
	l, ok := expr.L.(*ast.ColumnNameExpr)
	r, ok2 := expr.R.(*ast.ColumnNameExpr)
	if ok && ok2 {
		return TrueCondition{}, nil
	}
	value1, err1 = getValue(expr.L, args...)
	value2, err2 = getValue(expr.R, args...)
	if err1 == nil && err2 == nil {
		var result bool
		compareResult := misc.Compare(value1, value2)
		switch expr.Op {
		case opcode.EQ:
			result = compareResult == 0
		case opcode.NE:
			result = compareResult != 0
		case opcode.GT:
			result = compareResult > 0
		case opcode.GE:
			result = compareResult >= 0
		case opcode.LT:
			result = compareResult < 0
		case opcode.LE:
			result = compareResult <= 0
		default:
			log.Panicf("unsupported binary compare operation %s!", expr.Op)
		}

		if result {
			return TrueCondition{}, nil
		} else {
			return FalseCondition{}, nil
		}
	}

	if ok {
		if err2 != nil {
			return nil, err2
		}
		key = l
		value = value2
		op = expr.Op
	}
	if ok2 {
		if err1 != nil {
			return nil, err1
		}
		key = r
		value = value1
		switch expr.Op {
		case opcode.EQ:
			op = expr.Op
		case opcode.NE:
			op = expr.Op
		case opcode.LT:
			op = opcode.GT
		case opcode.LE:
			op = opcode.GE
		case opcode.GT:
			op = opcode.LT
		case opcode.GE:
			op = opcode.LE
		default:
			log.Panicf("unsupported binary compare operation %s!", expr.Op)
		}
	}
	return &KeyCondition{
		Key:   key.Name.String(),
		Op:    op,
		Value: value,
	}, nil
}

func ParseMathCondition(expr *ast.BinaryOperationExpr, args ...interface{}) (Condition, error) {
	val, err := function.Eval(expr, args...)
	if err != nil {
		return nil, err
	}
	if misc.IsZero(val) {
		return FalseCondition{}, nil
	}
	return TrueCondition{}, nil
}

func ParseLogicalCondition(expr *ast.BinaryOperationExpr, args ...interface{}) (Condition, error) {
	left, err := ParseCondition(expr.L, args...)
	if err != nil {
		return nil, err
	}
	right, err := ParseCondition(expr.R, args...)
	if err != nil {
		return nil, err
	}

	switch expr.Op {
	case opcode.LogicOr:
		return left.Or(right), nil
	default:
		return left.And(right), nil
	}
}

func ParseLikeCondition(expr *ast.PatternLikeExpr, args ...interface{}) (Condition, error) {
	switch key := expr.Expr.(type) {
	case *ast.ColumnNameExpr:
		if right, err := getValue(expr.Pattern, args...); err == nil {
			if like, ok := right.(string); ok {
				return &KeyCondition{
					Key:   key.Name.String(),
					Op:    opcode.Like,
					Value: like,
				}, nil
			}
		}

	}
	return nil, nil
}

func ParseBetweenCondition(expr *ast.BetweenExpr, args ...interface{}) (Condition, error) {
	var result []Condition
	switch key := expr.Expr.(type) {
	case *ast.ColumnNameExpr:
		lv, err := getValue(expr.Left, args...)
		if err != nil {
			return nil, err
		}

		rv, err := getValue(expr.Right, args...)
		if err != nil {
			return nil, err
		}

		if expr.Not {
			result = append(result, &KeyCondition{
				Key:   key.Name.String(),
				Op:    opcode.LT,
				Value: lv,
			})
			result = append(result, &KeyCondition{
				Key:   key.Name.String(),
				Op:    opcode.GT,
				Value: rv,
			})
			return &ComplexCondition{
				Op:         opcode.Or,
				Conditions: result,
			}, nil
		} else {
			result = append(result, &KeyCondition{
				Key:   key.Name.String(),
				Op:    opcode.GE,
				Value: lv,
			})
			result = append(result, &KeyCondition{
				Key:   key.Name.String(),
				Op:    opcode.LE,
				Value: rv,
			})
			return &ComplexCondition{
				Op:         opcode.And,
				Conditions: result,
			}, nil
		}
	}

	return nil, nil
}

func ParseInCondition(expr *ast.PatternInExpr, args ...interface{}) (Condition, error) {
	var result []Condition
	switch key := expr.Expr.(type) {
	case *ast.ColumnNameExpr:
		for _, exp := range expr.List {
			switch item := exp.(type) {
			case *driver.ParamMarkerExpr, *driver.ValueExpr:
				actualValue, err := getValue(item, args...)
				if err != nil {
					return nil, err
				}
				if expr.Not {
					result = append(result, &KeyCondition{
						Key:   key.Name.String(),
						Op:    opcode.NE,
						Value: actualValue,
					})
				} else {
					result = append(result, &KeyCondition{
						Key:   key.Name.String(),
						Op:    opcode.EQ,
						Value: actualValue,
					})
				}
			default:
				return nil, errors.Errorf("unsupported %t expression within pattern in expr", item)
			}
		}

		return &ComplexCondition{
			Op:         opcode.Or,
			Conditions: result,
		}, nil
	}

	return nil, nil
}

func ParseValCondition(expr ast.ExprNode, args ...interface{}) (Condition, error) {
	switch e := expr.(type) {
	case *ast.UnaryOperationExpr:
		val, err := getValue(e, args...)
		if err != nil {
			return nil, err
		}
		if misc.IsZero(val) {
			return FalseCondition{}, nil
		}
		return TrueCondition{}, nil
	case *driver.ValueExpr:
		if misc.IsZero(e.GetValue()) {
			return FalseCondition{}, nil
		}
		return TrueCondition{}, nil
	default:
		return nil, errors.Errorf("unsupported parse val expression %T", expr)
	}
}

func getValue(expr ast.ExprNode, args ...interface{}) (interface{}, error) {
	switch val := expr.(type) {
	case *driver.ValueExpr:
		return val.GetValue(), nil
	case *driver.ParamMarkerExpr:
		return args[val.Order], nil
	case *ast.UnaryOperationExpr:
		v, err := getValue(val.V, args...)
		if err != nil {
			return nil, err
		}
		return misc.ComputeUnary(val.Op, v)
	case *ast.BinaryOperationExpr:
		return function.Eval(val, args...)
	case *ast.FuncCallExpr:
		return function.EvalFunction(val, args...)
	case *ast.FuncCastExpr:
		return function.EvalCastFunction(val, args...)
	case *ast.CaseExpr:
		return function.EvalCaseWhenFunction(val, args...)
	case *ast.ParenthesesExpr:
		nested, ok := val.Expr.(*ast.BinaryOperationExpr)
		if ok {
			switch nested.Op {
			case opcode.Plus, opcode.Minus, opcode.Mul, opcode.Div:
				return getValue(nested, args...)
			}
		}
		return nil, errors.Errorf("only support nest expressions within mathematical binary operation expr")
	default:
		return nil, errors.Errorf("unsupported %t expression", expr)
	}
}
