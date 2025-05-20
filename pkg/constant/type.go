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

package constant

import (
	"fmt"

	"github.com/cectc/dbpack/third_party/parser/mysql"
)

// Flags sent from the MySQL C API
type MySqlFlag int32

const (
	MySqlFlag_EMPTY                 MySqlFlag = 0
	MySqlFlag_NOT_NULL_FLAG         MySqlFlag = 1
	MySqlFlag_PRI_KEY_FLAG          MySqlFlag = 2
	MySqlFlag_UNIQUE_KEY_FLAG       MySqlFlag = 4
	MySqlFlag_MULTIPLE_KEY_FLAG     MySqlFlag = 8
	MySqlFlag_BLOB_FLAG             MySqlFlag = 16
	MySqlFlag_UNSIGNED_FLAG         MySqlFlag = 32
	MySqlFlag_ZEROFILL_FLAG         MySqlFlag = 64
	MySqlFlag_BINARY_FLAG           MySqlFlag = 128
	MySqlFlag_ENUM_FLAG             MySqlFlag = 256
	MySqlFlag_AUTO_INCREMENT_FLAG   MySqlFlag = 512
	MySqlFlag_TIMESTAMP_FLAG        MySqlFlag = 1024
	MySqlFlag_SET_FLAG              MySqlFlag = 2048
	MySqlFlag_NO_DEFAULT_VALUE_FLAG MySqlFlag = 4096
	MySqlFlag_ON_UPDATE_NOW_FLAG    MySqlFlag = 8192
	MySqlFlag_NUM_FLAG              MySqlFlag = 32768
	MySqlFlag_PART_KEY_FLAG         MySqlFlag = 16384
	MySqlFlag_GROUP_FLAG            MySqlFlag = 32768
	MySqlFlag_UNIQUE_FLAG           MySqlFlag = 65536
	MySqlFlag_BINCMP_FLAG           MySqlFlag = 131072
)

// Enum value maps for MySqlFlag.
var (
	MySqlFlag_name = map[int32]string{
		0:     "EMPTY",
		1:     "NOT_NULL_FLAG",
		2:     "PRI_KEY_FLAG",
		4:     "UNIQUE_KEY_FLAG",
		8:     "MULTIPLE_KEY_FLAG",
		16:    "BLOB_FLAG",
		32:    "UNSIGNED_FLAG",
		64:    "ZEROFILL_FLAG",
		128:   "BINARY_FLAG",
		256:   "ENUM_FLAG",
		512:   "AUTO_INCREMENT_FLAG",
		1024:  "TIMESTAMP_FLAG",
		2048:  "SET_FLAG",
		4096:  "NO_DEFAULT_VALUE_FLAG",
		8192:  "ON_UPDATE_NOW_FLAG",
		32768: "NUM_FLAG",
		16384: "PART_KEY_FLAG",
		// Duplicate value: 32768: "GROUP_FLAG",
		65536:  "UNIQUE_FLAG",
		131072: "BINCMP_FLAG",
	}
	MySqlFlag_value = map[string]int32{
		"EMPTY":                 0,
		"NOT_NULL_FLAG":         1,
		"PRI_KEY_FLAG":          2,
		"UNIQUE_KEY_FLAG":       4,
		"MULTIPLE_KEY_FLAG":     8,
		"BLOB_FLAG":             16,
		"UNSIGNED_FLAG":         32,
		"ZEROFILL_FLAG":         64,
		"BINARY_FLAG":           128,
		"ENUM_FLAG":             256,
		"AUTO_INCREMENT_FLAG":   512,
		"TIMESTAMP_FLAG":        1024,
		"SET_FLAG":              2048,
		"NO_DEFAULT_VALUE_FLAG": 4096,
		"ON_UPDATE_NOW_FLAG":    8192,
		"NUM_FLAG":              32768,
		"PART_KEY_FLAG":         16384,
		"GROUP_FLAG":            32768,
		"UNIQUE_FLAG":           65536,
		"BINCMP_FLAG":           131072,
	}
)

func (x MySqlFlag) Enum() *MySqlFlag {
	p := new(MySqlFlag)
	*p = x
	return p
}

func (x MySqlFlag) String() string {
	return MySqlFlag_name[int32(x)]
}

// https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnType
type FieldType byte

const (
	FieldTypeDecimal FieldType = iota
	FieldTypeTiny
	FieldTypeShort
	FieldTypeLong
	FieldTypeFloat
	FieldTypeDouble
	FieldTypeNULL
	FieldTypeTimestamp
	FieldTypeLongLong
	FieldTypeInt24
	FieldTypeDate
	FieldTypeTime
	FieldTypeDateTime
	FieldTypeYear
	FieldTypeNewDate
	FieldTypeVarChar
	FieldTypeBit
)

const (
	FieldTypeUint8 FieldType = iota + 0x85
	FieldTypeUint16
	FieldTypeUint24
	FieldTypeUint32
	FieldTypeUint64
)

const (
	FieldTypeJSON FieldType = iota + 0xf5
	FieldTypeNewDecimal
	FieldTypeEnum
	FieldTypeSet
	FieldTypeTinyBLOB
	FieldTypeMediumBLOB
	FieldTypeLongBLOB
	FieldTypeBLOB
	FieldTypeVarString
	FieldTypeString
	FieldTypeGeometry
)

// If you add to this map, make sure you add a test case
// in tabletserver/endtoend.
var mysqlToType = map[int64]FieldType{
	0:   FieldTypeDecimal,
	1:   FieldTypeTiny,
	2:   FieldTypeShort,
	3:   FieldTypeLong,
	4:   FieldTypeFloat,
	5:   FieldTypeDouble,
	6:   FieldTypeNULL,
	7:   FieldTypeTimestamp,
	8:   FieldTypeLongLong,
	9:   FieldTypeInt24,
	10:  FieldTypeDate,
	11:  FieldTypeTime,
	12:  FieldTypeDateTime,
	13:  FieldTypeYear,
	14:  FieldTypeNewDate,
	15:  FieldTypeVarChar,
	16:  FieldTypeBit,
	17:  FieldTypeTimestamp,
	18:  FieldTypeDateTime,
	19:  FieldTypeTime,
	245: FieldTypeJSON,
	246: FieldTypeDecimal,
	247: FieldTypeEnum,
	248: FieldTypeSet,
	249: FieldTypeTinyBLOB,
	250: FieldTypeMediumBLOB,
	251: FieldTypeLongBLOB,
	252: FieldTypeBLOB,
	253: FieldTypeVarString,
	254: FieldTypeString,
	255: FieldTypeGeometry,
}

// modifyType modifies the vitess type based on the
// mysql flag. The function checks specific flags based
// on the type. This allows us to ignore stray flags
// that MySQL occasionally sets.
func modifyType(typ FieldType, flags int64) FieldType {
	switch typ {
	case FieldTypeTiny:
		if uint(flags)&UnsignedFlag != 0 {
			return FieldTypeUint8
		}
		return FieldTypeTiny
	case FieldTypeShort:
		if uint(flags)&UnsignedFlag != 0 {
			return FieldTypeUint16
		}
		return FieldTypeShort
	case FieldTypeLong:
		if uint(flags)&UnsignedFlag != 0 {
			return FieldTypeUint32
		}
		return FieldTypeLong
	case FieldTypeLongLong:
		if uint(flags)&UnsignedFlag != 0 {
			return FieldTypeUint64
		}
		return FieldTypeLongLong
	case FieldTypeInt24:
		if uint(flags)&UnsignedFlag != 0 {
			return FieldTypeUint24
		}
		return FieldTypeInt24
	}
	return typ
}

// MySQLToType computes the vitess type from mysql type and flags.
func MySQLToType(mysqlType, flags int64) (typ FieldType, err error) {
	result, ok := mysqlToType[mysqlType]
	if !ok {
		return 0, fmt.Errorf("unsupported type: %d", mysqlType)
	}
	return modifyType(result, flags), nil
}

// typeToMySQL is the reverse of MysqlToType.
var typeToMySQL = map[FieldType]struct {
	typ   int64
	flags int64
}{
	FieldTypeTiny:       {typ: 1},
	FieldTypeUint8:      {typ: 1, flags: int64(mysql.UnsignedFlag)},
	FieldTypeShort:      {typ: 2},
	FieldTypeUint16:     {typ: 2, flags: int64(mysql.UnsignedFlag)},
	FieldTypeLong:       {typ: 3},
	FieldTypeUint32:     {typ: 3, flags: int64(mysql.UnsignedFlag)},
	FieldTypeFloat:      {typ: 4},
	FieldTypeDouble:     {typ: 5},
	FieldTypeNULL:       {typ: 6, flags: int64(mysql.BinaryFlag)},
	FieldTypeTimestamp:  {typ: 7},
	FieldTypeLongLong:   {typ: 8},
	FieldTypeUint64:     {typ: 8, flags: int64(mysql.UnsignedFlag)},
	FieldTypeInt24:      {typ: 9},
	FieldTypeUint24:     {typ: 9, flags: int64(mysql.UnsignedFlag)},
	FieldTypeDate:       {typ: 10, flags: int64(mysql.BinaryFlag)},
	FieldTypeTime:       {typ: 11, flags: int64(mysql.BinaryFlag)},
	FieldTypeDateTime:   {typ: 12, flags: int64(mysql.BinaryFlag)},
	FieldTypeYear:       {typ: 13, flags: int64(mysql.UnsignedFlag)},
	FieldTypeBit:        {typ: 16, flags: int64(mysql.UnsignedFlag)},
	FieldTypeJSON:       {typ: 245},
	FieldTypeDecimal:    {typ: 246},
	FieldTypeEnum:       {typ: 247},
	FieldTypeSet:        {typ: 248},
	FieldTypeTinyBLOB:   {typ: 249},
	FieldTypeMediumBLOB: {typ: 250},
	FieldTypeLongBLOB:   {typ: 251},
	FieldTypeBLOB:       {typ: 252},
	FieldTypeVarString:  {typ: 253},
	FieldTypeString:     {typ: 254},
	FieldTypeGeometry:   {typ: 255},
}

// TypeToMySQL returns the equivalent mysql type and flag for a vitess type.
func TypeToMySQL(typ FieldType) (mysqlType, flags int64) {
	val := typeToMySQL[typ]
	return val.typ, val.flags
}

// TypeUnspecified is an uninitialized type. TypeDecimal is not used in MySQL.
const TypeUnspecified = FieldTypeDecimal

// Flag information.
const (
	NotNullFlag        uint = 1 << 0  /* Field can't be NULL */
	PriKeyFlag         uint = 1 << 1  /* Field is part of a primary key */
	UniqueKeyFlag      uint = 1 << 2  /* Field is part of a unique key */
	MultipleKeyFlag    uint = 1 << 3  /* Field is part of a key */
	BlobFlag           uint = 1 << 4  /* Field is a blob */
	UnsignedFlag       uint = 1 << 5  /* Field is unsigned */
	ZerofillFlag       uint = 1 << 6  /* Field is zerofill */
	BinaryFlag         uint = 1 << 7  /* Field is binary   */
	EnumFlag           uint = 1 << 8  /* Field is an enum */
	AutoIncrementFlag  uint = 1 << 9  /* Field is an auto increment field */
	TimestampFlag      uint = 1 << 10 /* Field is a timestamp */
	SetFlag            uint = 1 << 11 /* Field is a set */
	NoDefaultValueFlag uint = 1 << 12 /* Field doesn't have a default value */
	OnUpdateNowFlag    uint = 1 << 13 /* Field is set to NOW on UPDATE */
	PartKeyFlag        uint = 1 << 14 /* Intern: Part of some keys */
	NumFlag            uint = 1 << 15 /* Field is a num (for clients) */

	GroupFlag             uint = 1 << 15 /* Internal: Group field */
	UniqueFlag            uint = 1 << 16 /* Internal: Used by sql_yacc */
	BinCmpFlag            uint = 1 << 17 /* Internal: Used by sql_yacc */
	ParseToJSONFlag       uint = 1 << 18 /* Internal: Used when we want to parse string to JSON in CAST */
	IsBooleanFlag         uint = 1 << 19 /* Internal: Used for telling boolean literal from integer */
	PreventNullInsertFlag uint = 1 << 20 /* Prevent this Field from inserting NULL values */
)

// TypeInt24 bounds.
const (
	MaxUint24 = 1<<24 - 1
	MaxInt24  = 1<<23 - 1
	MinInt24  = -1 << 23
)

// HasNotNullFlag checks if NotNullFlag is set.
func HasNotNullFlag(flag uint) bool {
	return (flag & NotNullFlag) > 0
}

// HasNoDefaultValueFlag checks if NoDefaultValueFlag is set.
func HasNoDefaultValueFlag(flag uint) bool {
	return (flag & NoDefaultValueFlag) > 0
}

// HasAutoIncrementFlag checks if AutoIncrementFlag is set.
func HasAutoIncrementFlag(flag uint) bool {
	return (flag & AutoIncrementFlag) > 0
}

// HasUnsignedFlag checks if UnsignedFlag is set.
func HasUnsignedFlag(flag uint) bool {
	return (flag & UnsignedFlag) > 0
}

// HasZerofillFlag checks if ZerofillFlag is set.
func HasZerofillFlag(flag uint) bool {
	return (flag & ZerofillFlag) > 0
}

// HasBinaryFlag checks if BinaryFlag is set.
func HasBinaryFlag(flag uint) bool {
	return (flag & BinaryFlag) > 0
}

// HasPriKeyFlag checks if PriKeyFlag is set.
func HasPriKeyFlag(flag uint) bool {
	return (flag & PriKeyFlag) > 0
}

// HasUniKeyFlag checks if UniqueKeyFlag is set.
func HasUniKeyFlag(flag uint) bool {
	return (flag & UniqueKeyFlag) > 0
}

// HasMultipleKeyFlag checks if MultipleKeyFlag is set.
func HasMultipleKeyFlag(flag uint) bool {
	return (flag & MultipleKeyFlag) > 0
}

// HasTimestampFlag checks if HasTimestampFlag is set.
func HasTimestampFlag(flag uint) bool {
	return (flag & TimestampFlag) > 0
}

// HasOnUpdateNowFlag checks if OnUpdateNowFlag is set.
func HasOnUpdateNowFlag(flag uint) bool {
	return (flag & OnUpdateNowFlag) > 0
}

// HasParseToJSONFlag checks if ParseToJSONFlag is set.
func HasParseToJSONFlag(flag uint) bool {
	return (flag & ParseToJSONFlag) > 0
}

// HasIsBooleanFlag checks if IsBooleanFlag is set.
func HasIsBooleanFlag(flag uint) bool {
	return (flag & IsBooleanFlag) > 0
}

// HasPreventNullInsertFlag checks if PreventNullInsertFlag is set.
func HasPreventNullInsertFlag(flag uint) bool {
	return (flag & PreventNullInsertFlag) > 0
}
