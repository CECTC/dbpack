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

	"github.com/cectc/dbpack/pkg/visitor"
	"github.com/cectc/dbpack/third_party/parser"
	"github.com/cectc/dbpack/third_party/parser/ast"
)

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
