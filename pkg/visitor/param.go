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

package visitor

import (
	"github.com/cectc/dbpack/third_party/parser/ast"
	driver "github.com/cectc/dbpack/third_party/types/parser_driver"
)

type ParamVisitor struct {
	order int
}

func (v *ParamVisitor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	if param, ok := in.(*driver.ParamMarkerExpr); ok {
		param.SetOrder(v.order)
		v.order++
	}
	return in, false
}

func (v *ParamVisitor) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}

type FuncColumn struct {
	FuncName    string
	ColumnIndex int
}

type FuncVisitor struct {
	FuncColumns []*FuncColumn
	order       int
}

func (v *FuncVisitor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	if field, ok := in.(*ast.SelectField); ok {
		if f, is := field.Expr.(*ast.AggregateFuncExpr); is {
			v.FuncColumns = append(v.FuncColumns, &FuncColumn{
				FuncName:    f.F,
				ColumnIndex: v.order,
			})
		}
		v.order++
	}
	return in, false
}

func (v *FuncVisitor) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}
