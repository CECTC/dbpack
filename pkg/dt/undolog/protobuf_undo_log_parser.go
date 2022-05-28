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

package undolog

import (
	"bytes"
	"fmt"
	"log"
	"reflect"
	"time"

	"google.golang.org/protobuf/proto"
	"vimagination.zapto.org/byteio"

	"github.com/cectc/dbpack/pkg/constant"
	"github.com/cectc/dbpack/pkg/dt/schema"
	"github.com/cectc/dbpack/pkg/misc"
)

type ProtoBufUndoLogParser struct {
}

func (parser ProtoBufUndoLogParser) GetName() string {
	return "protobuf"
}

func (parser ProtoBufUndoLogParser) GetDefaultContent() []byte {
	return []byte("[]")
}

func (parser ProtoBufUndoLogParser) Encode(branchUndoLog *BranchUndoLog) []byte {
	pbBranchUndoLog := convertBranchSqlUndoLog(branchUndoLog)
	data, err := proto.Marshal(pbBranchUndoLog)
	if err != nil {
		log.Panic(err)
	}
	return data
}

func (parser ProtoBufUndoLogParser) Decode(data []byte) *BranchUndoLog {
	var pbBranchUndoLog = &PbBranchUndoLog{}
	err := proto.Unmarshal(data, pbBranchUndoLog)
	if err != nil {
		log.Panic(err)
	}

	return convertPbBranchSqlUndoLog(pbBranchUndoLog)
}

func (parser ProtoBufUndoLogParser) EncodeSqlUndoLog(undoLog *SqlUndoLog) []byte {
	pbSqlUndoLog := convertSqlUndoLog(undoLog)
	data, err := proto.Marshal(pbSqlUndoLog)
	if err != nil {
		log.Panic(err)
	}
	return data
}

func (parser ProtoBufUndoLogParser) DecodeSqlUndoLog(data []byte) *SqlUndoLog {
	var pbSqlUndoLog = &PbSqlUndoLog{}
	if err := proto.Unmarshal(data, pbSqlUndoLog); err != nil {
		log.Panic(err)
	}

	return convertPbSqlUndoLog(pbSqlUndoLog)
}

func convertField(field *schema.Field) *PbField {
	pbField := &PbField{
		Name:    field.Name,
		KeyType: int32(field.KeyType),
		Type:    field.Type,
	}
	if field.Value == nil {
		return pbField
	}
	var buf bytes.Buffer
	w := byteio.BigEndianWriter{Writer: &buf}

	switch v := field.Value.(type) {
	case int64:
		w.WriteByte(byte(constant.FieldTypeLongLong))
		w.WriteInt64(v)
		break
	case float32:
		w.WriteByte(byte(constant.FieldTypeFloat))
		w.WriteFloat32(v)
		break
	case float64:
		w.WriteByte(byte(constant.FieldTypeDouble))
		w.WriteFloat64(v)
		break
	case string:
		w.WriteByte(byte(constant.FieldTypeString))
		w.WriteString(v)
		break
	case []uint8:
		w.WriteByte(byte(constant.FieldTypeString))
		w.Write(v)
		break
	case time.Time:
		var a [64]byte
		var b = a[:0]

		if v.IsZero() {
			b = append(b, "0000-00-00"...)
		} else {
			loc, _ := time.LoadLocation("Local")
			b = v.In(loc).AppendFormat(b, constant.TimeFormat)
		}
		w.WriteByte(byte(constant.FieldTypeTime))
		w.Write(b)
	default:
		log.Panicf("unsupported types:%s,%v", reflect.TypeOf(field.Value).String(), field.Value)
	}
	pbField.Value = buf.Bytes()
	return pbField
}

func convertPbField(pbField *PbField) *schema.Field {
	field := &schema.Field{
		Name:    pbField.Name,
		KeyType: schema.KeyType(pbField.KeyType),
		Type:    pbField.Type,
	}
	if pbField.Value == nil {
		return field
	}
	r := byteio.BigEndianReader{Reader: bytes.NewReader(pbField.Value)}
	valueType, _ := r.ReadByte()

	switch constant.FieldType(valueType) {
	case constant.FieldTypeLongLong:
		value, _, _ := r.ReadInt64()
		field.Value = value
	case constant.FieldTypeFloat:
		value, _, _ := r.ReadFloat32()
		field.Value = value
	case constant.FieldTypeDouble:
		value, _, _ := r.ReadFloat64()
		field.Value = value
	case constant.FieldTypeString:
		field.Value = pbField.Value[1:]
	case constant.FieldTypeTime:
		loc, _ := time.LoadLocation("Local")
		t, err := misc.ParseDateTime(
			pbField.Value[1:],
			loc,
		)
		if err != nil {
			log.Panic(err)
		}
		field.Value = t
		break
	default:
		fmt.Printf("unsupport types:%v", valueType)
		break
	}
	return field
}

func convertRow(row *schema.Row) *PbRow {
	pbFields := make([]*PbField, 0)
	for _, field := range row.Fields {
		pbField := convertField(field)
		pbFields = append(pbFields, pbField)
	}
	pbRow := &PbRow{
		Fields: pbFields,
	}
	return pbRow
}

func convertPbRow(pbRow *PbRow) *schema.Row {
	fields := make([]*schema.Field, 0)
	for _, pbField := range pbRow.Fields {
		field := convertPbField(pbField)
		fields = append(fields, field)
	}
	row := &schema.Row{Fields: fields}
	return row
}

func convertTableRecords(records *schema.TableRecords) *PbTableRecords {
	pbRows := make([]*PbRow, 0)
	for _, row := range records.Rows {
		pbRow := convertRow(row)
		pbRows = append(pbRows, pbRow)
	}
	pbRecords := &PbTableRecords{
		TableName: records.TableName,
		Rows:      pbRows,
	}
	return pbRecords
}

func convertPbTableRecords(pbRecords *PbTableRecords) *schema.TableRecords {
	rows := make([]*schema.Row, 0)
	for _, pbRow := range pbRecords.Rows {
		row := convertPbRow(pbRow)
		rows = append(rows, row)
	}
	records := &schema.TableRecords{
		TableName: pbRecords.TableName,
		Rows:      rows,
	}
	return records
}

func convertSqlUndoLog(undoLog *SqlUndoLog) *PbSqlUndoLog {
	pbSqlUndoLog := &PbSqlUndoLog{
		IsBinary:   undoLog.IsBinary,
		SqlType:    int32(undoLog.SqlType),
		SchemaName: undoLog.SchemaName,
		TableName:  undoLog.TableName,
		LockKey:    undoLog.LockKey,
	}
	if undoLog.BeforeImage != nil {
		beforeImage := convertTableRecords(undoLog.BeforeImage)
		pbSqlUndoLog.BeforeImage = beforeImage
	}
	if undoLog.AfterImage != nil {
		afterImage := convertTableRecords(undoLog.AfterImage)
		pbSqlUndoLog.AfterImage = afterImage
	}

	return pbSqlUndoLog
}

func convertPbSqlUndoLog(pbSqlUndoLog *PbSqlUndoLog) *SqlUndoLog {
	sqlUndoLog := &SqlUndoLog{
		IsBinary:   pbSqlUndoLog.IsBinary,
		SqlType:    constant.SQLType(pbSqlUndoLog.SqlType),
		SchemaName: pbSqlUndoLog.SchemaName,
		TableName:  pbSqlUndoLog.TableName,
		LockKey:    pbSqlUndoLog.LockKey,
	}
	if pbSqlUndoLog.BeforeImage != nil {
		beforeImage := convertPbTableRecords(pbSqlUndoLog.BeforeImage)
		sqlUndoLog.BeforeImage = beforeImage
	}
	if pbSqlUndoLog.AfterImage != nil {
		afterImage := convertPbTableRecords(pbSqlUndoLog.AfterImage)
		sqlUndoLog.AfterImage = afterImage
	}
	return sqlUndoLog
}

func convertBranchSqlUndoLog(branchUndoLog *BranchUndoLog) *PbBranchUndoLog {
	sqlUndoLogs := make([]*PbSqlUndoLog, 0)
	for _, sqlUndoLog := range branchUndoLog.SqlUndoLogs {
		pbSqlUndoLog := convertSqlUndoLog(sqlUndoLog)
		sqlUndoLogs = append(sqlUndoLogs, pbSqlUndoLog)
	}
	pbBranchUndoLog := &PbBranchUndoLog{
		Xid:         branchUndoLog.Xid,
		BranchID:    branchUndoLog.BranchID,
		SqlUndoLogs: sqlUndoLogs,
	}
	return pbBranchUndoLog
}

func convertPbBranchSqlUndoLog(pbBranchUndoLog *PbBranchUndoLog) *BranchUndoLog {
	sqlUndoLogs := make([]*SqlUndoLog, 0)
	for _, sqlUndoLog := range pbBranchUndoLog.SqlUndoLogs {
		sqlUndoLog := convertPbSqlUndoLog(sqlUndoLog)
		sqlUndoLogs = append(sqlUndoLogs, sqlUndoLog)
	}
	branchUndoLog := &BranchUndoLog{
		Xid:         pbBranchUndoLog.Xid,
		BranchID:    pbBranchUndoLog.BranchID,
		SqlUndoLogs: sqlUndoLogs,
	}
	return branchUndoLog
}
