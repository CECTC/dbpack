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
