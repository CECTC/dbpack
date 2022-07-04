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
	"testing"

	"github.com/stretchr/testify/assert"

	err2 "github.com/cectc/dbpack/pkg/errors"
	"github.com/cectc/dbpack/pkg/visitor"
	"github.com/cectc/dbpack/third_party/parser"
	"github.com/cectc/dbpack/third_party/parser/ast"
)

func TestBuildCaseWhenFunction(t *testing.T) {
	p := parser.New()
	stmt, err := p.ParseOneStmt(`SELECT film_id, title, rental_rate, rental_duration 
		FROM film 
		WHERE rental_duration = CASE rental_rate
		                            WHEN 0.99 THEN 3
		                            WHEN 2.99 THEN 4
		                            WHEN 4.99 THEN 5
		                            ELSE 6
		                        END 
		ORDER BY title DESC;`, "", "")
	if err != nil {
		panic(err.Error())
	}
	stmt.Accept(&visitor.ParamVisitor{})
	sel, _ := stmt.(*ast.SelectStmt)
	s, err := globalCalculator.buildCaseWhenFunction(sel.Where.(*ast.BinaryOperationExpr).R.(*ast.CaseExpr))
	assert.ErrorIs(t, err2.CannotEvalWithColumnName, err)
	assert.Equal(t, "", s)

	stmt, err = p.ParseOneStmt(`SELECT film_id, title, rental_rate, rental_duration 
		FROM film 
		WHERE rental_duration = CASE 1 + 1
		                            WHEN 0 THEN 3
		                            WHEN 1 THEN 4
		                            WHEN 2 THEN 5
		                            ELSE 6
		                        END 
		ORDER BY title DESC;`, "", "")
	if err != nil {
		panic(err.Error())
	}
	stmt.Accept(&visitor.ParamVisitor{})
	sel, _ = stmt.(*ast.SelectStmt)
	s, err = globalCalculator.buildCaseWhenFunction(sel.Where.(*ast.BinaryOperationExpr).R.(*ast.CaseExpr))
	assert.Nil(t, err)
	assert.Equal(t, "$_IF(0 == (1 +1), 3, $_IF(1 == (1 +1), 4, $_IF(2 == (1 +1), 5, 6)))", s)
}

func TestBuildCastFunction(t *testing.T) {
	p := parser.New()
	stmt, err := p.ParseOneStmt(`SELECT film_id, title, rental_rate, rental_duration 
		FROM film 
		WHERE rental_duration = (3 + CAST('3' AS SIGNED))/2`, "", "")
	if err != nil {
		panic(err.Error())
	}
	stmt.Accept(&visitor.ParamVisitor{})
	sel, _ := stmt.(*ast.SelectStmt)
	s, err := globalCalculator.buildMathOperation(sel.Where.(*ast.BinaryOperationExpr).R.(*ast.BinaryOperationExpr))
	assert.Nil(t, err)
	assert.Equal(t, "(3 +$_CAST_SIGNED(\"3\")) /2", s)
	v, err := EvalString(s)
	assert.Nil(t, err)
	assert.Equal(t, int64(3), v)

	stmt, err = p.ParseOneStmt(`SELECT film_id, title, rental_rate, rental_duration 
		FROM film 
		WHERE rental_duration = CAST(6 AS CHAR)`, "", "")
	if err != nil {
		panic(err.Error())
	}
	stmt.Accept(&visitor.ParamVisitor{})
	sel, _ = stmt.(*ast.SelectStmt)
	s, err = globalCalculator.buildCastFunction(sel.Where.(*ast.BinaryOperationExpr).R.(*ast.FuncCastExpr))
	assert.Nil(t, err)
	assert.Equal(t, "$_CAST_CHAR(0, 'utf8mb4', 6)", s)
	v, err = EvalString(s)
	assert.Nil(t, err)
	assert.Equal(t, "6", v)
}

func TestBuildFunction(t *testing.T) {
	p := parser.New()
	stmt, err := p.ParseOneStmt(`SELECT film_id, title, rental_rate, rental_duration 
		FROM film 
		WHERE rental_duration = POW(2,2)`, "", "")
	if err != nil {
		panic(err.Error())
	}
	stmt.Accept(&visitor.ParamVisitor{})
	sel, _ := stmt.(*ast.SelectStmt)
	s, err := globalCalculator.buildFunction(sel.Where.(*ast.BinaryOperationExpr).R.(*ast.FuncCallExpr))
	assert.Nil(t, err)
	assert.Equal(t, "$_POW(2, 2)", s)
	v, err := EvalString(s)
	assert.Nil(t, err)
	assert.Equal(t, int64(4), v)
}

func TestEval(t *testing.T) {
	v, err := Eval(buildMathOperationExpr(), true)
	assert.NoError(t, err, "eval failed")
	t.Log("eval result:", v)
}

func BenchmarkEval(b *testing.B) {
	expr := buildMathOperationExpr()
	args := []interface{}{1}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = Eval(expr, args...)
		}
	})
}

func buildMathOperationExpr() *ast.BinaryOperationExpr {
	p := parser.New()
	stmt, err := p.ParseOneStmt("select * from t where a = 1 + if(?,1,0)", "", "")
	if err != nil {
		panic(err.Error())
	}
	stmt.Accept(&visitor.ParamVisitor{})
	sel, _ := stmt.(*ast.SelectStmt)

	return sel.Where.(*ast.BinaryOperationExpr).R.(*ast.BinaryOperationExpr)
}
