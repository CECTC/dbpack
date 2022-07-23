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
	"io"
	"strings"

	"github.com/cectc/dbpack/pkg/mysql"
	"github.com/cectc/dbpack/pkg/proto"
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

func decodeTextResult(result proto.Result) (proto.Result, error) {
	if result != nil {
		if mysqlResult, ok := result.(*mysql.Result); ok {
			if mysqlResult.Rows != nil {
				var rows []proto.Row
				for {
					row, err := mysqlResult.Rows.Next()
					if err != nil {
						if err == io.EOF {
							break
						}
						return nil, err
					}
					textRow := &mysql.TextRow{Row: row}
					_, err = textRow.Decode()
					if err != nil {
						return nil, err
					}
					rows = append(rows, textRow)
				}
				decodedRow := &mysql.DecodedResult{
					Fields:       mysqlResult.Fields,
					AffectedRows: mysqlResult.AffectedRows,
					InsertId:     mysqlResult.InsertId,
					Rows:         rows,
				}
				return decodedRow, nil
			}
		}
	}
	return result, nil
}

func decodeBinaryResult(result proto.Result) (proto.Result, error) {
	if result != nil {
		if mysqlResult, ok := result.(*mysql.Result); ok {
			if mysqlResult.Rows != nil {
				var rows []proto.Row
				for {
					row, err := mysqlResult.Rows.Next()
					if err != nil {
						if err == io.EOF {
							break
						}
						return nil, err
					}
					binaryRow := &mysql.BinaryRow{Row: row}
					_, err = binaryRow.Decode()
					if err != nil {
						return nil, err
					}
					rows = append(rows, binaryRow)
				}
				decodedRow := &mysql.DecodedResult{
					Fields:       mysqlResult.Fields,
					AffectedRows: mysqlResult.AffectedRows,
					InsertId:     mysqlResult.InsertId,
					Rows:         rows,
				}
				return decodedRow, nil
			}
		}
	}
	return result, nil
}
