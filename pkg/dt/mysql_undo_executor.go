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

package dt

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"

	"github.com/cectc/dbpack/pkg/constant"
	"github.com/cectc/dbpack/pkg/dt/schema"
	"github.com/cectc/dbpack/pkg/dt/undolog"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/misc"
	"github.com/cectc/dbpack/pkg/mysql"
	"github.com/cectc/dbpack/pkg/proto"
)

const (
	InsertSqlTemplate = "INSERT INTO %s (%s) VALUES (%s)"
	DeleteSqlTemplate = "DELETE FROM %s WHERE `%s` = ?"
	UpdateSqlTemplate = "UPDATE %s SET %s WHERE `%s` = ?"
	SelectSqlTemplate = "SELECT %s FROM %s WHERE `%s` IN %s"
)

type BuildUndoSql func(undoLog undolog.SqlUndoLog) string

func BuildDeleteUndoSql(undoLog *undolog.SqlUndoLog) string {
	beforeImage := undoLog.BeforeImage
	beforeImageRows := beforeImage.Rows

	if len(beforeImageRows) == 0 {
		return ""
	}

	row := beforeImageRows[0]
	fields := row.NonPrimaryKeys()
	pkField := row.PrimaryKeys()[0]
	// PK is at last one.
	fields = append(fields, pkField)

	var sbCols, sbVals strings.Builder
	var size = len(fields)
	for i, field := range fields {
		sbCols.WriteString(fmt.Sprintf("`%s`", field.Name))
		sbVals.WriteByte('?')
		if i < size-1 {
			sbCols.WriteString(", ")
			sbVals.WriteString(", ")
		}
	}
	insertColumns := sbCols.String()
	insertValues := sbVals.String()

	return fmt.Sprintf(InsertSqlTemplate, undoLog.TableName, insertColumns, insertValues)
}

func BuildInsertUndoSql(undoLog *undolog.SqlUndoLog) string {
	afterImage := undoLog.AfterImage
	afterImageRows := afterImage.Rows
	if len(afterImageRows) == 0 {
		return ""
	}
	row := afterImageRows[0]
	pkField := row.PrimaryKeys()[0]
	return fmt.Sprintf(DeleteSqlTemplate, undoLog.TableName, pkField.Name)
}

func BuildUpdateUndoSql(undoLog *undolog.SqlUndoLog) string {
	beforeImage := undoLog.BeforeImage
	beforeImageRows := beforeImage.Rows

	if len(beforeImageRows) == 0 {
		return ""
	}

	row := beforeImageRows[0]
	nonPkFields := row.NonPrimaryKeys()
	pkField := row.PrimaryKeys()[0]

	var sb strings.Builder
	var size = len(nonPkFields)
	for i, field := range nonPkFields {
		fmt.Fprintf(&sb, "`%s` = ?", field.Name)
		if i < size-1 {
			fmt.Fprint(&sb, ", ")
		}
	}
	updateColumns := sb.String()

	return fmt.Sprintf(UpdateSqlTemplate, undoLog.TableName, updateColumns, pkField.Name)
}

type MysqlUndoExecutor struct {
	sqlUndoLog *undolog.SqlUndoLog
}

func NewMysqlUndoExecutor(undoLog *undolog.SqlUndoLog) MysqlUndoExecutor {
	return MysqlUndoExecutor{sqlUndoLog: undoLog}
}

func (executor MysqlUndoExecutor) Execute(tx proto.Tx) error {
	goOn, err := executor.dataValidationAndGoOn(tx)
	if err != nil {
		return err
	}
	if !goOn {
		return nil
	}

	var undoSql string
	var undoRows schema.TableRecords

	// PK is at last one.
	// INSERT INTO a (x, y, z, pk) VALUES (?, ?, ?, ?)
	// UPDATE a SET x=?, y=?, z=? WHERE pk = ?
	// DELETE FROM a WHERE pk = ?
	switch executor.sqlUndoLog.SqlType {
	case constant.SQLType_INSERT:
		undoSql = BuildInsertUndoSql(executor.sqlUndoLog)
		undoRows = *executor.sqlUndoLog.AfterImage

	case constant.SQLType_DELETE:
		undoSql = BuildDeleteUndoSql(executor.sqlUndoLog)
		undoRows = *executor.sqlUndoLog.BeforeImage

	case constant.SQLType_UPDATE:
		undoSql = BuildUpdateUndoSql(executor.sqlUndoLog)
		undoRows = *executor.sqlUndoLog.BeforeImage

	default:
		panic(errors.Errorf("unsupport sql type:%s", executor.sqlUndoLog.SqlType.String()))
	}

	if undoSql == "" {
		return nil
	}

	for i := len(undoRows.Rows) - 1; i >= 0; i-- {
		var args = make([]interface{}, 0)
		var pkValue interface{}

		row := undoRows.Rows[i]
		for _, field := range row.Fields {
			if field.KeyType == schema.PrimaryKey {
				pkValue = field.Value
			} else {
				if executor.sqlUndoLog.SqlType != constant.SQLType_INSERT {
					args = append(args, field.Value)
				}
			}
		}
		args = append(args, pkValue)
		_, _, err = tx.ExecuteSql(context.Background(), undoSql, args...)
		if err != nil {
			return err
		}
	}
	return nil
}

func (executor MysqlUndoExecutor) dataValidationAndGoOn(tx proto.Tx) (bool, error) {
	if executor.sqlUndoLog.BeforeImage != nil && executor.sqlUndoLog.AfterImage == nil {
		return true, nil
	}
	beforeEqualsAfterResult := cmp.Equal(executor.sqlUndoLog.BeforeImage, executor.sqlUndoLog.AfterImage)
	if beforeEqualsAfterResult {
		log.Info("Stop RollbackLocal because there is no data change between the before data snapshot and the after data snapshot.")
		return false, nil
	}
	currentRecords, err := executor.queryCurrentRecords(tx)
	if err != nil {
		return false, err
	}
	afterEqualsCurrentResult := cmp.Equal(executor.sqlUndoLog.AfterImage, currentRecords)
	if !afterEqualsCurrentResult {
		// If current data is not equivalent to the after data, then compare the current data with the before
		// data, too. No need continue to undo if current data is equivalent to the before data snapshot
		beforeEqualsCurrentResult := cmp.Equal(executor.sqlUndoLog.BeforeImage, currentRecords)
		if beforeEqualsCurrentResult {
			log.Info("Stop RollbackLocal because there is no data change between the before data snapshot and the after data snapshot.")
			return false, nil
		} else {
			oldRows, _ := json.Marshal(executor.sqlUndoLog.AfterImage.Rows)
			newRows, _ := json.Marshal(currentRecords.Rows)
			log.Errorf("check dirty datas failed, old and new data are not equal, tableName:[%s], oldRows:[%s], newRows:[%s].",
				executor.sqlUndoLog.TableName, string(oldRows), string(newRows))
			return false, errors.New("Has dirty records when undo.")
		}
	}
	return true, nil
}

func (executor MysqlUndoExecutor) queryCurrentRecords(tx proto.Tx) (*schema.TableRecords, error) {
	undoRecords := executor.sqlUndoLog.GetUndoRows()
	tableMeta := undoRecords.TableMeta
	pkName := tableMeta.GetPKName()

	pkFields := undoRecords.PKFields()
	if pkFields == nil || len(pkFields) == 0 {
		return nil, nil
	}

	var pkValues = make([]interface{}, 0)
	for _, field := range pkFields {
		pkValues = append(pkValues, field.Value)
	}

	if executor.sqlUndoLog.IsBinary {
		selectSql := executor.buildCurrentRecordsForPrepareSql(tableMeta, pkName, pkValues)
		dataTable, _, err := tx.ExecuteSql(context.Background(), selectSql, pkValues...)
		if err != nil {
			return nil, err
		}
		dt := dataTable.(*mysql.Result)
		return schema.BuildBinaryRecords(tableMeta, dt), nil
	} else {
		selectSql := executor.buildCurrentRecordsForQuerySql(tableMeta, pkName, pkValues)
		dataTable, _, err := tx.Query(context.Background(), selectSql)
		if err != nil {
			return nil, err
		}
		dt := dataTable.(*mysql.Result)
		return schema.BuildTextRecords(tableMeta, dt), nil
	}
}

func (executor MysqlUndoExecutor) buildCurrentRecordsForPrepareSql(tableMeta schema.TableMeta, pkColumn string, pkValues []interface{}) string {
	var b strings.Builder
	var i = 0
	columnCount := len(tableMeta.Columns)
	for _, columnName := range tableMeta.Columns {
		b.WriteString(misc.CheckAndReplace(columnName))
		i = i + 1
		if i < columnCount {
			b.WriteByte(',')
		} else {
			b.WriteByte(' ')
		}
	}
	inCondition := misc.MysqlAppendInParam(len(pkValues))
	return fmt.Sprintf(SelectSqlTemplate, b.String(), tableMeta.TableName, pkColumn, inCondition)
}

func (executor MysqlUndoExecutor) buildCurrentRecordsForQuerySql(tableMeta schema.TableMeta, pkColumn string, pkValues []interface{}) string {
	var columns strings.Builder
	var inCondition strings.Builder
	var i = 0
	columnCount := len(tableMeta.Columns)
	for _, columnName := range tableMeta.Columns {
		columns.WriteString(misc.CheckAndReplace(columnName))
		i = i + 1
		if i < columnCount {
			columns.WriteByte(',')
		} else {
			columns.WriteByte(' ')
		}
	}
	inCondition.WriteByte('(')
	for i, pk := range pkValues {
		if i < len(pkValues)-1 {
			inCondition.WriteString(fmt.Sprintf("'%s',", pk))
		} else {
			inCondition.WriteString(fmt.Sprintf("'%s'", pk))
		}
	}
	inCondition.WriteByte(')')
	return fmt.Sprintf(SelectSqlTemplate, columns.String(), tableMeta.TableName, pkColumn, inCondition.String())
}
