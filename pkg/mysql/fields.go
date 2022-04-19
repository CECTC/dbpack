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
	"database/sql"
	"reflect"

	"github.com/cectc/dbpack/pkg/constant"
)

var (
	scanTypeFloat32   = reflect.TypeOf(float32(0))
	scanTypeFloat64   = reflect.TypeOf(float64(0))
	scanTypeInt8      = reflect.TypeOf(int8(0))
	scanTypeInt16     = reflect.TypeOf(int16(0))
	scanTypeInt32     = reflect.TypeOf(int32(0))
	scanTypeInt64     = reflect.TypeOf(int64(0))
	scanTypeNullFloat = reflect.TypeOf(sql.NullFloat64{})
	scanTypeNullInt   = reflect.TypeOf(sql.NullInt64{})
	scanTypeNullTime  = reflect.TypeOf(sql.NullTime{})
	scanTypeUint8     = reflect.TypeOf(uint8(0))
	scanTypeUint16    = reflect.TypeOf(uint16(0))
	scanTypeUint32    = reflect.TypeOf(uint32(0))
	scanTypeUint64    = reflect.TypeOf(uint64(0))
	scanTypeRawBytes  = reflect.TypeOf(sql.RawBytes{})
	scanTypeUnknown   = reflect.TypeOf(new(interface{}))
)

type Field struct {
	Table        string
	OrgTable     string
	Database     string
	Name         string
	OrgName      string
	Length       uint32
	Flags        uint
	FieldType    constant.FieldType
	Decimals     byte
	CharSet      uint16
	ColumnLength uint32

	DefaultValueLength uint64
	DefaultValue       []byte
}

func (mf *Field) FiledName() string {
	return mf.Name
}

func (mf *Field) TableName() string {
	return mf.Table
}

func (mf *Field) DataBaseName() string {
	return mf.Database
}

func (mf *Field) TypeDatabaseName() string {
	switch mf.FieldType {
	case constant.FieldTypeBit:
		return "BIT"
	case constant.FieldTypeBLOB:
		if mf.CharSet != constant.Collations[constant.BinaryCollation] {
			return "TEXT"
		}
		return "BLOB"
	case constant.FieldTypeDate:
		return "DATE"
	case constant.FieldTypeDateTime:
		return "DATETIME"
	case constant.FieldTypeDecimal:
		return "DECIMAL"
	case constant.FieldTypeDouble:
		return "DOUBLE"
	case constant.FieldTypeEnum:
		return "ENUM"
	case constant.FieldTypeFloat:
		return "FLOAT"
	case constant.FieldTypeGeometry:
		return "GEOMETRY"
	case constant.FieldTypeInt24:
		return "MEDIUMINT"
	case constant.FieldTypeJSON:
		return "JSON"
	case constant.FieldTypeLong:
		return "INT"
	case constant.FieldTypeLongBLOB:
		if mf.CharSet != constant.Collations[constant.BinaryCollation] {
			return "LONGTEXT"
		}
		return "LONGBLOB"
	case constant.FieldTypeLongLong:
		return "BIGINT"
	case constant.FieldTypeMediumBLOB:
		if mf.CharSet != constant.Collations[constant.BinaryCollation] {
			return "MEDIUMTEXT"
		}
		return "MEDIUMBLOB"
	case constant.FieldTypeNewDate:
		return "DATE"
	case constant.FieldTypeNewDecimal:
		return "DECIMAL"
	case constant.FieldTypeNULL:
		return "NULL"
	case constant.FieldTypeSet:
		return "SET"
	case constant.FieldTypeShort:
		return "SMALLINT"
	case constant.FieldTypeString:
		if mf.CharSet == constant.Collations[constant.BinaryCollation] {
			return "BINARY"
		}
		return "CHAR"
	case constant.FieldTypeTime:
		return "TIME"
	case constant.FieldTypeTimestamp:
		return "TIMESTAMP"
	case constant.FieldTypeTiny:
		return "TINYINT"
	case constant.FieldTypeTinyBLOB:
		if mf.CharSet != constant.Collations[constant.BinaryCollation] {
			return "TINYTEXT"
		}
		return "TINYBLOB"
	case constant.FieldTypeVarChar:
		if mf.CharSet == constant.Collations[constant.BinaryCollation] {
			return "VARBINARY"
		}
		return "VARCHAR"
	case constant.FieldTypeVarString:
		if mf.CharSet == constant.Collations[constant.BinaryCollation] {
			return "VARBINARY"
		}
		return "VARCHAR"
	case constant.FieldTypeYear:
		return "YEAR"
	default:
		return ""
	}
}

func (mf *Field) scanType() reflect.Type {
	switch mf.FieldType {
	case constant.FieldTypeTiny:
		if mf.Flags&constant.NotNullFlag != 0 {
			if mf.Flags&constant.UnsignedFlag != 0 {
				return scanTypeUint8
			}
			return scanTypeInt8
		}
		return scanTypeNullInt

	case constant.FieldTypeShort, constant.FieldTypeYear:
		if mf.Flags&constant.NotNullFlag != 0 {
			if mf.Flags&constant.UnsignedFlag != 0 {
				return scanTypeUint16
			}
			return scanTypeInt16
		}
		return scanTypeNullInt

	case constant.FieldTypeInt24, constant.FieldTypeLong:
		if mf.Flags&constant.NotNullFlag != 0 {
			if mf.Flags&constant.UnsignedFlag != 0 {
				return scanTypeUint32
			}
			return scanTypeInt32
		}
		return scanTypeNullInt

	case constant.FieldTypeLongLong:
		if mf.Flags&constant.NotNullFlag != 0 {
			if mf.Flags&constant.UnsignedFlag != 0 {
				return scanTypeUint64
			}
			return scanTypeInt64
		}
		return scanTypeNullInt

	case constant.FieldTypeFloat:
		if mf.Flags&constant.NotNullFlag != 0 {
			return scanTypeFloat32
		}
		return scanTypeNullFloat

	case constant.FieldTypeDouble:
		if mf.Flags&constant.UnsignedFlag != 0 {
			return scanTypeFloat64
		}
		return scanTypeNullFloat

	case constant.FieldTypeDecimal, constant.FieldTypeNewDecimal, constant.FieldTypeVarChar,
		constant.FieldTypeBit, constant.FieldTypeEnum, constant.FieldTypeSet, constant.FieldTypeTinyBLOB,
		constant.FieldTypeMediumBLOB, constant.FieldTypeLongBLOB, constant.FieldTypeBLOB,
		constant.FieldTypeVarString, constant.FieldTypeString, constant.FieldTypeGeometry, constant.FieldTypeJSON,
		constant.FieldTypeTime:
		return scanTypeRawBytes

	case constant.FieldTypeDate, constant.FieldTypeNewDate,
		constant.FieldTypeTimestamp, constant.FieldTypeDateTime:
		// NullTime is always returned for more consistent behavior as it can
		// handle both cases of parseTime regardless if the field is nullable.
		return scanTypeNullTime

	default:
		return scanTypeUnknown
	}
}
