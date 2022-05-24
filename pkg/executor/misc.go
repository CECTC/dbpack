package executor

import (
	"strings"

	"github.com/cectc/dbpack/third_party/parser/ast"
	driver "github.com/cectc/dbpack/third_party/types/parser_driver"
)

const (
	autocommit = "autocommit"
	off        = "off"
)

func shouldStartTransaction(stmt *ast.SetStmt) (shouldStartTransaction bool) {
	if len(stmt.Variables) == 1 && strings.EqualFold(stmt.Variables[0].Name, autocommit) {
		switch exprType := stmt.Variables[0].Value.(type) {
		case *driver.ValueExpr:
			if exprType.GetValue() == int64(0) {
				shouldStartTransaction = true
			}
		case *ast.ColumnNameExpr:
			if strings.EqualFold(exprType.Name.String(), off) {
				shouldStartTransaction = true
			}
		}
	}
	return
}
