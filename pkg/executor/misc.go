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
