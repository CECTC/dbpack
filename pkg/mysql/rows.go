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

package mysql

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/cectc/dbpack/pkg/constant"
	"github.com/cectc/dbpack/pkg/errors"
	"github.com/cectc/dbpack/pkg/misc"
	"github.com/cectc/dbpack/pkg/packet"
	"github.com/cectc/dbpack/pkg/proto"
)

type ResultSet struct {
	Columns     []*Field
	ColumnNames []string
}

type Rows struct {
	conn    *Conn
	columns []*Field
}

func NewRows(conn *Conn, columns []*Field) *Rows {
	return &Rows{
		conn:    conn,
		columns: columns,
	}
}

func (rows *Rows) Next() (*Row, error) {
	data, err := rows.conn.ReadPacket()
	if err != nil {
		return nil, err
	}

	// EOF Packet
	if data[0] == constant.EOFPacket && len(data) == 5 {
		// server_status [2 bytes]
		misc.ReadUint16(data, 3)
		rows.conn = nil
		return nil, io.EOF
	}
	if data[0] == constant.ErrPacket {
		rows.conn = nil
		return nil, packet.ParseErrorPacket(data)
	}

	return &Row{
		Content: data,
		ResultSet: &ResultSet{
			Columns: rows.columns,
		},
	}, nil
}

type Row struct {
	Content   []byte
	ResultSet *ResultSet
}

type BinaryRow struct {
	*Row
	Values []*proto.Value
}

type TextRow struct {
	*Row
	Values []*proto.Value
}

func (row *Row) Columns() []string {
	if row.ResultSet.ColumnNames != nil {
		return row.ResultSet.ColumnNames
	}

	columns := make([]string, len(row.ResultSet.Columns))
	if row.Content != nil {
		for i := range columns {
			field := row.ResultSet.Columns[i]
			if tableName := field.Table; len(tableName) > 0 {
				columns[i] = tableName + "." + field.Name
			} else {
				columns[i] = field.Name
			}
		}
	} else {
		for i := range columns {
			field := row.ResultSet.Columns[i]
			columns[i] = field.Name
		}
	}

	row.ResultSet.ColumnNames = columns
	return columns
}

func (row *Row) Fields() []proto.Field {
	fields := make([]proto.Field, 0, len(row.ResultSet.Columns))
	for _, field := range row.ResultSet.Columns {
		fields = append(fields, field)
	}
	return fields
}

func (row *Row) Data() []byte {
	return row.Content
}

func (row *Row) Decode() ([]*proto.Value, error) {
	return nil, nil
}

func (rows *TextRow) Decode() ([]*proto.Value, error) {
	dest := make([]*proto.Value, len(rows.ResultSet.Columns))

	// RowSet Packet
	var val []byte
	var isNull bool
	var n int
	var err error
	pos := 0

	for i := 0; i < len(rows.ResultSet.Columns); i++ {
		field := rows.ResultSet.Columns[i]

		// Read bytes and convert to string
		val, isNull, n, err = misc.ReadLengthEncodedString(rows.Content[pos:])
		dest[i] = &proto.Value{
			Typ:   field.FieldType,
			Flags: field.Flags,
			Len:   n,
			Val:   val,
			Raw:   val,
		}
		pos += n
		if err == nil {
			if !isNull {
				switch field.FieldType {
				case constant.FieldTypeTimestamp, constant.FieldTypeDateTime,
					constant.FieldTypeDate, constant.FieldTypeNewDate:
					dest[i].Val, err = misc.ParseDateTime(
						val,
						time.Local,
					)
					if err == nil {
						continue
					}
				default:
					continue
				}
			} else {
				dest[i].Val = nil
				continue
			}
		}
		return nil, err // err != nil
	}
	rows.Values = dest
	return dest, nil
}

func (rows *BinaryRow) Decode() ([]*proto.Value, error) {
	dest := make([]*proto.Value, len(rows.ResultSet.Columns))

	if rows.Content[0] != constant.OKPacket {
		return nil, errors.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "read binary rows (%v) failed", rows)
	}

	// NULL-bitmap,  [(column-count + 7 + 2) / 8 bytes]
	pos := 1 + (len(dest)+7+2)>>3
	nullMask := rows.Content[1:pos]

	for i := 0; i < len(rows.ResultSet.Columns); i++ {
		// Field is NULL
		// (byte >> bit-pos) % 2 == 1
		if ((nullMask[(i+2)>>3] >> uint((i+2)&7)) & 1) == 1 {
			dest[i] = nil
			continue
		}

		field := rows.ResultSet.Columns[i]
		// Convert to byte-coded string
		switch field.FieldType {
		case constant.FieldTypeNULL:
			dest[i] = &proto.Value{
				Typ:   field.FieldType,
				Flags: field.Flags,
				Len:   1,
				Val:   nil,
				Raw:   nil,
			}
			continue

		// Numeric Types
		case constant.FieldTypeTiny:
			dest[i] = &proto.Value{
				Typ:   field.FieldType,
				Flags: field.Flags,
				Len:   1,
				Val:   int64(int8(rows.Content[pos])),
				Raw:   rows.Content[pos : pos+1],
			}
			pos++
			continue

		case constant.FieldTypeUint8:
			dest[i] = &proto.Value{
				Typ:   field.FieldType,
				Flags: field.Flags,
				Len:   1,
				Val:   int64(rows.Content[pos]),
				Raw:   rows.Content[pos : pos+1],
			}
			pos++
			continue

		case constant.FieldTypeShort, constant.FieldTypeYear:
			dest[i] = &proto.Value{
				Typ:   field.FieldType,
				Flags: field.Flags,
				Len:   2,
				Val:   int64(int16(binary.LittleEndian.Uint16(rows.Content[pos : pos+2]))),
				Raw:   rows.Content[pos : pos+1],
			}
			pos += 2
			continue

		case constant.FieldTypeUint16:
			dest[i] = &proto.Value{
				Typ:   field.FieldType,
				Flags: field.Flags,
				Len:   2,
				Val:   int64(binary.LittleEndian.Uint16(rows.Content[pos : pos+2])),
				Raw:   rows.Content[pos : pos+1],
			}
			pos += 2
			continue

		case constant.FieldTypeInt24, constant.FieldTypeLong:
			dest[i] = &proto.Value{
				Typ:   field.FieldType,
				Flags: field.Flags,
				Len:   4,
				Val:   int64(int32(binary.LittleEndian.Uint32(rows.Content[pos : pos+4]))),
				Raw:   rows.Content[pos : pos+4],
			}
			pos += 4
			continue

		case constant.FieldTypeUint24, constant.FieldTypeUint32:
			dest[i] = &proto.Value{
				Typ:   field.FieldType,
				Flags: field.Flags,
				Len:   4,
				Val:   int64(binary.LittleEndian.Uint32(rows.Content[pos : pos+4])),
				Raw:   rows.Content[pos : pos+4],
			}
			pos += 4
			continue

		case constant.FieldTypeLongLong:
			dest[i] = &proto.Value{
				Typ:   field.FieldType,
				Flags: field.Flags,
				Len:   8,
				Val:   int64(binary.LittleEndian.Uint64(rows.Content[pos : pos+8])),
				Raw:   rows.Content[pos : pos+8],
			}
			pos += 8
			continue

		case constant.FieldTypeUint64:
			val := binary.LittleEndian.Uint64(rows.Content[pos : pos+8])
			if val > math.MaxInt64 {
				dest[i] = &proto.Value{
					Typ:   field.FieldType,
					Flags: field.Flags,
					Len:   8,
					Val:   misc.Uint64ToString(val),
					Raw:   rows.Content[pos : pos+8],
				}
			} else {
				dest[i] = &proto.Value{
					Typ:   field.FieldType,
					Flags: field.Flags,
					Len:   8,
					Val:   int64(val),
					Raw:   rows.Content[pos : pos+8],
				}
			}
			pos += 8
			continue

		case constant.FieldTypeFloat:
			dest[i] = &proto.Value{
				Typ:   field.FieldType,
				Flags: field.Flags,
				Len:   4,
				Val:   math.Float32frombits(binary.LittleEndian.Uint32(rows.Content[pos : pos+4])),
				Raw:   rows.Content[pos : pos+4],
			}
			pos += 4
			continue

		case constant.FieldTypeDouble:
			dest[i] = &proto.Value{
				Typ:   field.FieldType,
				Flags: field.Flags,
				Len:   8,
				Val:   math.Float64frombits(binary.LittleEndian.Uint64(rows.Content[pos : pos+8])),
				Raw:   rows.Content[pos : pos+8],
			}
			pos += 8
			continue

		// Length coded Binary Strings
		case constant.FieldTypeDecimal, constant.FieldTypeNewDecimal, constant.FieldTypeVarChar,
			constant.FieldTypeBit, constant.FieldTypeEnum, constant.FieldTypeSet, constant.FieldTypeTinyBLOB,
			constant.FieldTypeMediumBLOB, constant.FieldTypeLongBLOB, constant.FieldTypeBLOB,
			constant.FieldTypeVarString, constant.FieldTypeString, constant.FieldTypeGeometry, constant.FieldTypeJSON:
			var val interface{}
			var isNull bool
			var n int
			var err error
			val, isNull, n, err = misc.ReadLengthEncodedString(rows.Content[pos:])
			dest[i] = &proto.Value{
				Typ:   field.FieldType,
				Flags: field.Flags,
				Len:   n,
				Val:   val,
				Raw:   rows.Content[pos : pos+n],
			}
			pos += n
			if err == nil {
				if !isNull {
					continue
				} else {
					dest[i].Val = nil
					continue
				}
			}
			return nil, err

		case
			constant.FieldTypeDate, constant.FieldTypeNewDate, // Date YYYY-MM-DD
			constant.FieldTypeTime,                                  // Time [-][H]HH:MM:SS[.fractal]
			constant.FieldTypeTimestamp, constant.FieldTypeDateTime: // Timestamp YYYY-MM-DD HH:MM:SS[.fractal]

			num, isNull, n := misc.ReadLengthEncodedInteger(rows.Content[pos:])
			pos += n

			var val interface{}
			var err error
			switch {
			case isNull:
				dest[i] = nil
				continue
			case field.FieldType == constant.FieldTypeTime:
				// database/sql does not support an equivalent to TIME, return a string
				var dstLen uint8
				switch decimals := field.Decimals; decimals {
				case 0x00, 0x1f:
					dstLen = 8
				case 1, 2, 3, 4, 5, 6:
					dstLen = 8 + 1 + decimals
				default:
					return nil, fmt.Errorf(
						"protocol error, illegal decimals architecture.Value %d",
						field.Decimals,
					)
				}
				val, err = misc.FormatBinaryTime(rows.Content[pos:pos+int(num)], dstLen)
				dest[i] = &proto.Value{
					Typ:   field.FieldType,
					Flags: field.Flags,
					Len:   n,
					Val:   val,
					Raw:   rows.Content[pos : pos+n],
				}
			default:
				val, err = misc.ParseBinaryDateTime(num, rows.Content[pos:], time.Local)
				dest[i] = &proto.Value{
					Typ:   field.FieldType,
					Flags: field.Flags,
					Len:   n,
					Val:   val,
					Raw:   rows.Content[pos : pos+n],
				}
				if err == nil {
					break
				}

				var dstlen uint8
				if field.FieldType == constant.FieldTypeDate {
					dstlen = 10
				} else {
					switch decimals := field.Decimals; decimals {
					case 0x00, 0x1f:
						dstlen = 19
					case 1, 2, 3, 4, 5, 6:
						dstlen = 19 + 1 + decimals
					default:
						return nil, fmt.Errorf(
							"protocol error, illegal decimals architecture.Value %d",
							field.Decimals,
						)
					}
				}
				val, err = misc.FormatBinaryDateTime(rows.Content[pos:pos+int(num)], dstlen)
				dest[i] = &proto.Value{
					Typ:   field.FieldType,
					Flags: field.Flags,
					Len:   n,
					Val:   val,
					Raw:   rows.Content[pos : pos+n],
				}
			}

			if err == nil {
				pos += int(num)
				continue
			} else {
				return nil, err
			}

		// Please report if this happens!
		default:
			return nil, fmt.Errorf("unknown field type %d", field.FieldType)
		}
	}
	rows.Values = dest
	return dest, nil
}
