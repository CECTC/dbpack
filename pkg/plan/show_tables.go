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

package plan

import (
	"context"
	"regexp"
	"strings"

	"github.com/pkg/errors"

	"github.com/cectc/dbpack/pkg/constant"
	"github.com/cectc/dbpack/pkg/mysql"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/third_party/parser/ast"
	"github.com/cectc/dbpack/third_party/parser/format"
)

const (
	tableNameRegex = `^(\w+)_(\d+)$`
)

var (
	tableNameRegexp = regexp.MustCompile(tableNameRegex)
)

type ShowTablesPlan struct {
	Stmt        *ast.ShowStmt
	Args        []interface{}
	Executor    proto.DBGroupExecutor
	LogicTables []string
}

func (p *ShowTablesPlan) Execute(ctx context.Context, _ ...*ast.TableOptimizerHint) (proto.Result, uint16, error) {
	var (
		sb     strings.Builder
		sql    string
		result proto.Result
		warns  uint16
		err    error
	)
	restoreCtx := format.NewRestoreCtx(constant.DBPackRestoreFormat, &sb)
	if err = p.Stmt.Restore(restoreCtx); err != nil {
		return nil, 0, errors.WithStack(err)
	}
	sql = sb.String()

	result, warns, err = p.Executor.Query(ctx, sql)
	if err != nil {
		return nil, 0, err
	}
	mysqlResult := result.(*mysql.Result)
	rows := make([]proto.Row, 0)
	rowMap := make(map[string]struct{})
	for _, row := range mysqlResult.Rows {
		if _, err := row.Decode(); err != nil {
			return nil, 0, err
		}
		textRow := row.(*mysql.TextRow)
		table, filtered := filterTable(string(textRow.Values[0].Val.([]byte)), p.LogicTables)
		if filtered {
			textRow.Values[0].Val = []byte(table)
		}
		if _, exist := rowMap[table]; !exist {
			rows = append(rows, textRow)
			rowMap[table] = struct{}{}
		}
	}
	return &mysql.Result{
		Fields:       mysqlResult.Fields,
		AffectedRows: mysqlResult.AffectedRows,
		InsertId:     mysqlResult.InsertId,
		Rows:         rows,
	}, warns, nil
}

// filterTable return table, filtered
func filterTable(table string, logicTables []string) (string, bool) {
	for _, logicTable := range logicTables {
		if tableNameRegexp.MatchString(table) {
			if strings.Contains(table, logicTable) {
				return logicTable, true
			}
		}
	}
	return table, false
}
