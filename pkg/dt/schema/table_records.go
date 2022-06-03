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

package schema

import (
	"fmt"
	"strings"

	"github.com/cectc/dbpack/pkg/mysql"
)

type TableRecords struct {
	TableMeta TableMeta `json:"-"`
	TableName string
	Rows      []*Row
}

type RowLock struct {
	XID      string
	BranchID int64
	RowKey   string
}

func NewTableRecords(meta TableMeta) *TableRecords {
	return &TableRecords{
		TableMeta: meta,
		TableName: meta.TableName,
		Rows:      make([]*Row, 0),
	}
}

func (records *TableRecords) PKFields() []*Field {
	pkRows := make([]*Field, 0)
	pk := records.TableMeta.GetPKName()
	for _, row := range records.Rows {
		for _, field := range row.Fields {
			if strings.EqualFold(field.Name, pk) {
				pkRows = append(pkRows, field)
				break
			}
		}
	}
	return pkRows
}

func BuildLockKey(lockKeyRecords *TableRecords) string {
	if lockKeyRecords == nil || lockKeyRecords.Rows == nil || len(lockKeyRecords.Rows) == 0 {
		return ""
	}

	var sb strings.Builder
	sb.WriteString(lockKeyRecords.TableName)
	sb.WriteByte(':')
	fields := lockKeyRecords.PKFields()
	length := len(fields)
	for i, field := range fields {
		_, isByte := field.Value.([]byte)
		if isByte {
			sb.WriteString(fmt.Sprintf("%s", field.Value))
		} else {
			sb.WriteString(fmt.Sprintf("%v", field.Value))
		}
		if i < length-1 {
			sb.WriteByte(',')
		}
	}
	return sb.String()
}

func BuildBinaryRecords(meta TableMeta, result *mysql.Result) *TableRecords {
	records := NewTableRecords(meta)
	rs := make([]*Row, 0)

	for {
		row, err := result.Rows.Next()
		if err != nil {
			break
		}

		binaryRow := mysql.BinaryRow{Row: row}
		values, err := binaryRow.Decode()
		if err != nil {
			break
		}
		fields := make([]*Field, 0, len(result.Fields))
		for i, col := range result.Fields {
			field := &Field{
				Name: col.FiledName(),
				Type: meta.AllColumns[col.FiledName()].DataType,
			}
			if values[i] != nil {
				field.Value = values[i].Val
			}
			if strings.EqualFold(col.FiledName(), meta.GetPKName()) {
				field.KeyType = PrimaryKey
			}
			fields = append(fields, field)
		}
		r := &Row{Fields: fields}
		rs = append(rs, r)
	}
	if len(rs) == 0 {
		return nil
	}
	records.Rows = rs
	return records
}

func BuildTextRecords(meta TableMeta, result *mysql.Result) *TableRecords {
	records := NewTableRecords(meta)
	rs := make([]*Row, 0)

	for {
		row, err := result.Rows.Next()
		if err != nil {
			break
		}

		textRow := mysql.TextRow{Row: row}
		values, err := textRow.Decode()
		if err != nil {
			break
		}
		fields := make([]*Field, 0, len(result.Fields))
		for i, col := range result.Fields {
			field := &Field{
				Name: col.FiledName(),
				Type: meta.AllColumns[col.FiledName()].DataType,
			}
			if values[i] != nil {
				field.Value = values[i].Val
			}
			if strings.EqualFold(col.FiledName(), meta.GetPKName()) {
				field.KeyType = PrimaryKey
			}
			fields = append(fields, field)
		}
		r := &Row{Fields: fields}
		rs = append(rs, r)
	}
	if len(rs) == 0 {
		return nil
	}
	records.Rows = rs
	return records
}
