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

package function

import (
	"strconv"
	"strings"

	"github.com/pkg/errors"

	"github.com/cectc/dbpack/pkg/constant"
	err2 "github.com/cectc/dbpack/pkg/errors"
	"github.com/cectc/dbpack/pkg/misc"
	"github.com/cectc/dbpack/third_party/parser/ast"
	driver "github.com/cectc/dbpack/third_party/types/parser_driver"
)

const _jsFuncPrefix = "$_"

var globalCalculator = &calculator{}

type calculator struct {
}

func (c *calculator) buildCaseWhenFunction(node *ast.CaseExpr) (string, error) {
	var sb strings.Builder
	if err := caseWhenFunc2script(&sb, node); err != nil {
		return "", err
	}
	return sb.String(), nil
}

func (c *calculator) buildCastFunction(node *ast.FuncCastExpr) (string, error) {
	var sb strings.Builder
	if err := castFunc2script(&sb, node); err != nil {
		return "", err
	}
	return sb.String(), nil
}

func (c *calculator) buildFunction(node *ast.FuncCallExpr) (string, error) {
	var sb strings.Builder
	if err := func2script(&sb, node); err != nil {
		return "", err
	}
	return sb.String(), nil
}

func caseWhenFunc2script(sb *strings.Builder, node *ast.CaseExpr) error {
	var caseScript string

	// convert CASE header to script
	// eg: CASE 2+1 WHEN 1 THEN 'A' WHEN 2 THEN 'B' WHEN 3 THEN 'C' ELSE '*' END
	// will be converted to: $IF(1 == (2+1), 'A', $IF(2 == (2+1), 'B', $IF(3 == (2+1), 'C', '*' )))
	if node.Value != nil {
		var b strings.Builder
		if err := expr2script(&b, node.Value); err != nil {
			return err
		}
		caseScript = b.String()
	}

	for i, branch := range node.WhenClauses {
		var (
			when = branch.Expr
			then = branch.Result
		)

		if i > 0 {
			sb.WriteString(", ")
		}

		writeFuncName(sb, "IF")

		sb.WriteByte('(')

		if err := handleArg(sb, when); err != nil {
			return err
		}

		// write CASE header
		if len(caseScript) > 0 {
			sb.WriteString(" == (")
			sb.WriteString(caseScript)
			sb.WriteByte(')')
		}

		sb.WriteString(", ")

		if err := handleArg(sb, then); err != nil {
			return err
		}
	}

	sb.WriteString(", ")

	if node.ElseClause != nil {
		if err := handleArg(sb, node.ElseClause); err != nil {
			return err
		}
	} else {
		sb.WriteString("null")
	}

	for i := 0; i < len(node.WhenClauses); i++ {
		sb.WriteByte(')')
	}

	return nil
}

func castFunc2script(sb *strings.Builder, node *ast.FuncCastExpr) error {
	switch node.Tp.Tp {
	case byte(constant.FieldTypeString):
		// TODO: support binary
		return errors.New("cast to binary is not supported yet")
	case byte(constant.FieldTypeVarString):
		if node.Tp.Flag&constant.BinaryFlag != 0 {
			// TODO: support binary
			return errors.New("cast to binary is not supported yet")
		} else {
			writeFuncName(sb, "CAST_CHAR")
			sb.WriteByte('(')
			if node.Tp.Flen > 0 {
				sb.WriteString(strconv.Itoa(node.Tp.Flen))
			} else {
				sb.WriteByte('0')
			}
			sb.WriteString(", ")

			if node.Tp.Charset != "" {
				sb.WriteByte('\'')
				sb.WriteString(misc.Escape(node.Tp.Charset, misc.EscapeSingleQuote))
				sb.WriteByte('\'')
			} else {
				sb.WriteString("''")
			}

			sb.WriteString(", ")
		}
	case byte(constant.FieldTypeDate):
		writeFuncName(sb, "CAST_DATE")
		sb.WriteByte('(')
	case byte(constant.FieldTypeYear):
		// TODO: support cast year
		return errors.New("cast to year is not supported yet")
	case byte(constant.FieldTypeDateTime):
		writeFuncName(sb, "CAST_DATETIME")
		sb.WriteByte('(')
	case byte(constant.FieldTypeDecimal):
		writeFuncName(sb, "CAST_DECIMAL")
		sb.WriteByte('(')
		if node.Tp.Flen > 0 {
			sb.WriteString(strconv.Itoa(node.Tp.Flen))
		} else {
			sb.WriteByte('0')
		}
		sb.WriteString(", ")

		if node.Tp.Decimal > 0 {
			sb.WriteString(strconv.Itoa(node.Tp.Decimal))
		} else {
			sb.WriteByte('0')
		}
		sb.WriteString(", ")
	case byte(constant.FieldTypeTime):
		writeFuncName(sb, "CAST_TIME")
		sb.WriteByte('(')
	case byte(constant.FieldTypeLongLong):
		if node.Tp.Flag&constant.UnsignedFlag != 0 {
			writeFuncName(sb, "CAST_UNSIGNED")
			sb.WriteByte('(')
		} else {
			writeFuncName(sb, "CAST_SIGNED")
			sb.WriteByte('(')
		}
	case byte(constant.FieldTypeJSON):
		return errors.New("cast to json is not supported yet")
	case byte(constant.FieldTypeDouble):
		return errors.New("cast to double is not supported yet")
	case byte(constant.FieldTypeFloat):
		return errors.New("cast to float is not supported yet")
	}

	if err := expr2script(sb, node.Expr); err != nil {
		return err
	}

	sb.WriteByte(')')

	return nil
}

func func2script(sb *strings.Builder, node *ast.FuncCallExpr) error {
	if strings.ToLower(node.FnName.String()) == "convert" {
		charset := node.Args[1].(ast.ValueExpr).GetString()
		writeFuncName(sb, "CAST_CHARSET(")

		sb.WriteByte('\'')
		sb.WriteString(misc.Escape(charset, misc.EscapeSingleQuote))
		sb.WriteByte('\'')

		expr := node.Args[0]
		if err := expr2script(sb, expr); err != nil {
			return err
		}

		sb.WriteString(", ")
	} else {
		writeFuncName(sb, strings.ToUpper(node.FnName.String()))
		sb.WriteByte('(')
		for i, arg := range node.Args {
			if i > 0 {
				sb.WriteByte(',')
				sb.WriteByte(' ')
			}
			if err := handleArg(sb, arg); err != nil {
				return err
			}
		}
		sb.WriteByte(')')
	}
	return nil
}

// Eval calculates the result of math expression with custom args.
func Eval(node *ast.BinaryOperationExpr, args ...interface{}) (interface{}, error) {
	s, err := globalCalculator.buildMathOperation(node)
	if err != nil {
		return nil, err
	}
	return EvalString(s, args...)
}

func (c *calculator) buildMathOperation(node *ast.BinaryOperationExpr) (string, error) {
	var sb strings.Builder
	if err := mathOperation2script(&sb, node); err != nil {
		return "", err
	}
	return sb.String(), nil
}

func mathOperation2script(sb *strings.Builder, node *ast.BinaryOperationExpr) error {
	if err := expr2script(sb, node.L); err != nil {
		return err
	}
	sb.WriteByte(' ')
	node.Op.Format(sb)
	if err := expr2script(sb, node.R); err != nil {
		return err
	}

	return nil
}

func expr2script(sb *strings.Builder, node ast.ExprNode) error {
	switch v := node.(type) {
	case *ast.BinaryOperationExpr:
		if err := mathOperation2script(sb, v); err != nil {
			return err
		}
	case *driver.ValueExpr:
		v.Format(sb)
	case *ast.UnaryOperationExpr:
		sb.WriteString(funcUnary)
		sb.WriteString("('")
		v.Op.Format(sb)
		sb.WriteString("', ")
		if err := expr2script(sb, v.V); err != nil {
			return err
		}
		sb.WriteByte(')')
	case *ast.ColumnNameExpr:
		return err2.CannotEvalWithColumnName
	case *ast.ParenthesesExpr:
		sb.WriteByte('(')
		if err := expr2script(sb, v.Expr); err != nil {
			return err
		}
		sb.WriteByte(')')
	case *ast.FuncCallExpr:
		if err := func2script(sb, v); err != nil {
			return err
		}
	case *ast.AggregateFuncExpr:
		return errors.New("aggregate function should not appear here")
	case *ast.FuncCastExpr:
		if err := castFunc2script(sb, v); err != nil {
			return err
		}
	case *ast.CaseExpr:
		if err := caseWhenFunc2script(sb, v); err != nil {
			return err
		}
	default:
		return errors.Errorf("expression within %T is not supported yet", v)
	}
	return nil
}

func EvalCaseWhenFunction(node *ast.CaseExpr, args ...interface{}) (interface{}, error) {
	s, err := globalCalculator.buildCaseWhenFunction(node)
	if err != nil {
		return nil, err
	}
	return EvalString(s, args...)
}

func EvalCastFunction(node *ast.FuncCastExpr, args ...interface{}) (interface{}, error) {
	s, err := globalCalculator.buildCastFunction(node)
	if err != nil {
		return nil, err
	}
	return EvalString(s, args...)
}

// EvalFunction calculates the result of math expression with custom args.
func EvalFunction(node *ast.FuncCallExpr, args ...interface{}) (interface{}, error) {
	s, err := globalCalculator.buildFunction(node)
	if err != nil {
		return nil, err
	}
	return EvalString(s, args...)
}

// EvalString computes the result of given expression script with custom args.
func EvalString(script string, args ...interface{}) (interface{}, error) {
	js := BorrowJsRuntime()
	defer ReturnJsRuntime(js)
	return js.Eval(script, args)
}

func handleArg(sb *strings.Builder, arg ast.ExprNode) error {
	switch v := arg.(type) {
	case *ast.ColumnNameExpr:
		return err2.CannotEvalWithColumnName
	case *driver.ValueExpr:
		v.Format(sb)
	case *driver.ParamMarkerExpr:
		sb.WriteString("arguments[")
		sb.WriteString(strconv.Itoa(v.Order))
		sb.WriteByte(']')
	case *ast.BinaryOperationExpr:
		if err := mathOperation2script(sb, v); err != nil {
			return err
		}
	case *ast.FuncCallExpr:
		if err := func2script(sb, v); err != nil {
			return err
		}
	case *ast.FuncCastExpr:
		if err := castFunc2script(sb, v); err != nil {
			return err
		}
	case *ast.CaseExpr:
		if err := caseWhenFunc2script(sb, v); err != nil {
			return err
		}
	}
	return nil
}

func writeFuncName(sb *strings.Builder, name string) {
	sb.WriteString(_jsFuncPrefix)
	sb.WriteString(name)
}
