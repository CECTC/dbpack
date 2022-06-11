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

package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cectc/dbpack/third_party/parser/ast"
	"github.com/cectc/dbpack/third_party/parser/model"
	driver "github.com/cectc/dbpack/third_party/types/parser_driver"
)

func TestShouldStartTransaction(t *testing.T) {
	stmt := &ast.SetStmt{Variables: nil}
	assert.False(t, shouldStartTransaction(stmt))

	stmt.Variables = []*ast.VariableAssignment{
		{
			Name: "something",
		},
	}
	assert.False(t, shouldStartTransaction(stmt))

	valueExpr := driver.ValueExpr{
		TexprNode: ast.TexprNode{},
	}
	valueExpr.SetValue(0)

	stmt.Variables[0].Name = autocommit
	stmt.Variables[0].Value = &valueExpr

	assert.True(t, shouldStartTransaction(stmt))

	columnNameExpr := ast.ColumnNameExpr{
		Name: &ast.ColumnName{
			Name: model.CIStr{
				O: "",
				L: "off",
			},
		},
		Refer: nil,
	}
	stmt.Variables[0].Value = &columnNameExpr
	assert.True(t, shouldStartTransaction(stmt))
}
