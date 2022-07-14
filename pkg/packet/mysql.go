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

package packet

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/cectc/dbpack/pkg/constant"
	err2 "github.com/cectc/dbpack/pkg/errors"
	"github.com/cectc/dbpack/pkg/misc"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/third_party/parser/mysql"
)

func ParseComStmtExecute(stmts *sync.Map, data []byte) (uint32, byte, error) {
	pos := 0
	payload := data[1:]
	bitMap := make([]byte, 0)

	// statement ID
	stmtID, pos, ok := misc.ReadUint32(payload, 0)
	if !ok {
		return 0, 0, err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "reading statement ID failed")
	}
	p, ok := stmts.Load(stmtID)
	if !ok {
		return 0, 0, err2.NewSQLError(constant.CRCommandsOutOfSync, constant.SSUnknownSQLState, "statement ID is not found from record")
	}
	prepare := p.(*proto.Stmt)

	// cursor type flags
	cursorType, pos, ok := misc.ReadByte(payload, pos)
	if !ok {
		return stmtID, 0, err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "reading cursor type flags failed")
	}

	// iteration count
	iterCount, pos, ok := misc.ReadUint32(payload, pos)
	if !ok {
		return stmtID, 0, err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "reading iteration count failed")
	}
	if iterCount != uint32(1) {
		return stmtID, 0, err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "iteration count is not equal to 1")
	}

	if prepare.ParamsCount > 0 {
		bitMap, pos, ok = misc.ReadBytes(payload, pos, int((prepare.ParamsCount+7)/8))
		if !ok {
			return stmtID, 0, err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "reading NULL-bitmap failed")
		}
	}

	newParamsBoundFlag, pos, ok := misc.ReadByte(payload, pos)
	if ok && newParamsBoundFlag == 0x01 {
		var mysqlType, flags byte
		for i := uint16(0); i < prepare.ParamsCount; i++ {
			mysqlType, pos, ok = misc.ReadByte(payload, pos)
			if !ok {
				return stmtID, 0, err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "reading parameter type failed")
			}

			flags, pos, ok = misc.ReadByte(payload, pos)
			if !ok {
				return stmtID, 0, err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "reading parameter flags failed")
			}

			// convert MySQL type to internal type.
			valType, err := constant.MySQLToType(int64(mysqlType), int64(flags))
			if err != nil {
				return stmtID, 0, err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "MySQLToType(%v,%v) failed: %v", mysqlType, flags, err)
			}

			prepare.ParamsType[i] = int32(valType)
		}
	}

	for i := 0; i < len(prepare.ParamsType); i++ {
		var val interface{}
		parameterID := fmt.Sprintf("v%d", i+1)
		if v, ok := prepare.BindVars[parameterID]; ok {
			if v != nil {
				continue
			}
		}

		if (bitMap[i/8] & (1 << uint(i%8))) > 0 {
			val, pos, ok = ParseStmtArgs(nil, constant.FieldTypeNULL, pos)
		} else {
			val, pos, ok = ParseStmtArgs(payload, constant.FieldType(prepare.ParamsType[i]), pos)
		}
		if !ok {
			return stmtID, 0, err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "decoding parameter value failed: %v", prepare.ParamsType[i])
		}

		prepare.BindVars[parameterID] = val
	}

	return stmtID, cursorType, nil
}

func ParseStmtArgs(data []byte, typ constant.FieldType, pos int) (interface{}, int, bool) {
	switch typ {
	case constant.FieldTypeNULL:
		return nil, pos, true
	case constant.FieldTypeTiny:
		val, pos, ok := misc.ReadByte(data, pos)
		return int64(int8(val)), pos, ok
	case constant.FieldTypeUint8:
		val, pos, ok := misc.ReadByte(data, pos)
		return int64(int8(val)), pos, ok
	case constant.FieldTypeUint16:
		val, pos, ok := misc.ReadUint16(data, pos)
		return int64(int16(val)), pos, ok
	case constant.FieldTypeShort, constant.FieldTypeYear:
		val, pos, ok := misc.ReadUint16(data, pos)
		return int64(int16(val)), pos, ok
	case constant.FieldTypeUint24, constant.FieldTypeUint32:
		val, pos, ok := misc.ReadUint32(data, pos)
		return int64(val), pos, ok
	case constant.FieldTypeInt24, constant.FieldTypeLong:
		val, pos, ok := misc.ReadUint32(data, pos)
		return int64(int32(val)), pos, ok
	case constant.FieldTypeFloat:
		val, pos, ok := misc.ReadUint32(data, pos)
		return math.Float32frombits(uint32(val)), pos, ok
	case constant.FieldTypeUint64:
		val, pos, ok := misc.ReadUint64(data, pos)
		return val, pos, ok
	case constant.FieldTypeLongLong:
		val, pos, ok := misc.ReadUint64(data, pos)
		return int64(val), pos, ok
	case constant.FieldTypeDouble:
		val, pos, ok := misc.ReadUint64(data, pos)
		return math.Float64frombits(val), pos, ok
	case constant.FieldTypeTimestamp, constant.FieldTypeDate, constant.FieldTypeDateTime:
		size, pos, ok := misc.ReadByte(data, pos)
		if !ok {
			return nil, 0, false
		}
		switch size {
		case 0x00:
			return []byte{' '}, pos, ok
		case 0x0b:
			year, pos, ok := misc.ReadUint16(data, pos)
			if !ok {
				return nil, 0, false
			}
			month, pos, ok := misc.ReadByte(data, pos)
			if !ok {
				return nil, 0, false
			}
			day, pos, ok := misc.ReadByte(data, pos)
			if !ok {
				return nil, 0, false
			}
			hour, pos, ok := misc.ReadByte(data, pos)
			if !ok {
				return nil, 0, false
			}
			minute, pos, ok := misc.ReadByte(data, pos)
			if !ok {
				return nil, 0, false
			}
			second, pos, ok := misc.ReadByte(data, pos)
			if !ok {
				return nil, 0, false
			}
			microSecond, pos, ok := misc.ReadUint32(data, pos)
			if !ok {
				return nil, 0, false
			}
			val := strconv.Itoa(int(year)) + "-" +
				strconv.Itoa(int(month)) + "-" +
				strconv.Itoa(int(day)) + " " +
				strconv.Itoa(int(hour)) + ":" +
				strconv.Itoa(int(minute)) + ":" +
				strconv.Itoa(int(second)) + "." +
				fmt.Sprintf("%06d", microSecond)

			return []byte(val), pos, ok
		case 0x07:
			year, pos, ok := misc.ReadUint16(data, pos)
			if !ok {
				return nil, 0, false
			}
			month, pos, ok := misc.ReadByte(data, pos)
			if !ok {
				return nil, 0, false
			}
			day, pos, ok := misc.ReadByte(data, pos)
			if !ok {
				return nil, 0, false
			}
			hour, pos, ok := misc.ReadByte(data, pos)
			if !ok {
				return nil, 0, false
			}
			minute, pos, ok := misc.ReadByte(data, pos)
			if !ok {
				return nil, 0, false
			}
			second, pos, ok := misc.ReadByte(data, pos)
			if !ok {
				return nil, 0, false
			}
			val := strconv.Itoa(int(year)) + "-" +
				strconv.Itoa(int(month)) + "-" +
				strconv.Itoa(int(day)) + " " +
				strconv.Itoa(int(hour)) + ":" +
				strconv.Itoa(int(minute)) + ":" +
				strconv.Itoa(int(second))

			return []byte(val), pos, ok
		case 0x04:
			year, pos, ok := misc.ReadUint16(data, pos)
			if !ok {
				return nil, 0, false
			}
			month, pos, ok := misc.ReadByte(data, pos)
			if !ok {
				return nil, 0, false
			}
			day, pos, ok := misc.ReadByte(data, pos)
			if !ok {
				return nil, 0, false
			}
			val := strconv.Itoa(int(year)) + "-" +
				strconv.Itoa(int(month)) + "-" +
				strconv.Itoa(int(day))

			return []byte(val), pos, ok
		default:
			return nil, 0, false
		}
	case constant.FieldTypeTime:
		size, pos, ok := misc.ReadByte(data, pos)
		if !ok {
			return nil, 0, false
		}
		switch size {
		case 0x00:
			return []byte("00:00:00"), pos, ok
		case 0x0c:
			isNegative, pos, ok := misc.ReadByte(data, pos)
			if !ok {
				return nil, 0, false
			}
			days, pos, ok := misc.ReadUint32(data, pos)
			if !ok {
				return nil, 0, false
			}
			hour, pos, ok := misc.ReadByte(data, pos)
			if !ok {
				return nil, 0, false
			}

			hours := uint32(hour) + days*uint32(24)

			minute, pos, ok := misc.ReadByte(data, pos)
			if !ok {
				return nil, 0, false
			}
			second, pos, ok := misc.ReadByte(data, pos)
			if !ok {
				return nil, 0, false
			}
			microSecond, pos, ok := misc.ReadUint32(data, pos)
			if !ok {
				return nil, 0, false
			}

			val := ""
			if isNegative == 0x01 {
				val += "-"
			}
			val += strconv.Itoa(int(hours)) + ":" +
				strconv.Itoa(int(minute)) + ":" +
				strconv.Itoa(int(second)) + "." +
				fmt.Sprintf("%06d", microSecond)

			return []byte(val), pos, ok
		case 0x08:
			isNegative, pos, ok := misc.ReadByte(data, pos)
			if !ok {
				return nil, 0, false
			}
			days, pos, ok := misc.ReadUint32(data, pos)
			if !ok {
				return nil, 0, false
			}
			hour, pos, ok := misc.ReadByte(data, pos)
			if !ok {
				return nil, 0, false
			}

			hours := uint32(hour) + days*uint32(24)

			minute, pos, ok := misc.ReadByte(data, pos)
			if !ok {
				return nil, 0, false
			}
			second, pos, ok := misc.ReadByte(data, pos)
			if !ok {
				return nil, 0, false
			}

			val := ""
			if isNegative == 0x01 {
				val += "-"
			}
			val += strconv.Itoa(int(hours)) + ":" +
				strconv.Itoa(int(minute)) + ":" +
				strconv.Itoa(int(second))

			return []byte(val), pos, ok
		default:
			return nil, 0, false
		}
	case constant.FieldTypeDecimal, constant.FieldTypeNewDecimal, constant.FieldTypeVarChar, constant.FieldTypeTinyBLOB,
		constant.FieldTypeMediumBLOB, constant.FieldTypeLongBLOB, constant.FieldTypeBLOB, constant.FieldTypeVarString,
		constant.FieldTypeString, constant.FieldTypeGeometry, constant.FieldTypeJSON, constant.FieldTypeBit,
		constant.FieldTypeEnum, constant.FieldTypeSet:
		val, pos, ok := misc.ReadLenEncStringAsBytesCopy(data, pos)
		return val, pos, ok
	default:
		return nil, pos, false
	}
}

func TextVal2MySQL(v *proto.Value) ([]byte, error) {
	var out []byte
	pos := 0
	if v == nil {
		return out, nil
	}

	switch v.Typ {
	case constant.FieldTypeNULL:
		// no-op
	case constant.FieldTypeTiny:
		val, err := strconv.ParseInt(fmt.Sprintf("%s", v.Val), 10, 8)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 1)
		misc.WriteByte(out, pos, uint8(val))
	case constant.FieldTypeUint8:
		val, err := strconv.ParseUint(fmt.Sprintf("%s", v.Val), 10, 8)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 1)
		misc.WriteByte(out, pos, uint8(val))
	case constant.FieldTypeUint16:
		val, err := strconv.ParseUint(fmt.Sprintf("%s", v.Val), 10, 16)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 2)
		misc.WriteUint16(out, pos, uint16(val))
	case constant.FieldTypeShort, constant.FieldTypeYear:
		val, err := strconv.ParseInt(fmt.Sprintf("%s", v.Val), 10, 16)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 2)
		misc.WriteUint16(out, pos, uint16(val))
	case constant.FieldTypeUint24, constant.FieldTypeUint32:
		val, err := strconv.ParseUint(fmt.Sprintf("%s", v.Val), 10, 32)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 4)
		misc.WriteUint32(out, pos, uint32(val))
	case constant.FieldTypeInt24, constant.FieldTypeLong:
		val, err := strconv.ParseInt(fmt.Sprintf("%s", v.Val), 10, 32)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 4)
		misc.WriteUint32(out, pos, uint32(val))
	case constant.FieldTypeFloat:
		val, err := strconv.ParseFloat(fmt.Sprintf("%s", v.Val), 32)
		if err != nil {
			return []byte{}, err
		}
		bits := math.Float32bits(float32(val))
		out = make([]byte, 4)
		misc.WriteUint32(out, pos, bits)
	case constant.FieldTypeUint64:
		val, err := strconv.ParseUint(fmt.Sprintf("%s", v.Val), 10, 64)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 8)
		misc.WriteUint64(out, pos, uint64(val))
	case constant.FieldTypeLongLong:
		val, err := strconv.ParseInt(fmt.Sprintf("%s", v.Val), 10, 64)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 8)
		misc.WriteUint64(out, pos, uint64(val))
	case constant.FieldTypeDouble:
		val, err := strconv.ParseFloat(fmt.Sprintf("%s", v.Val), 64)
		if err != nil {
			return []byte{}, err
		}
		bits := math.Float64bits(val)
		out = make([]byte, 8)
		misc.WriteUint64(out, pos, bits)
	case constant.FieldTypeTimestamp, constant.FieldTypeDate, constant.FieldTypeDateTime:
		if len(v.Raw) > 19 {
			out = make([]byte, 1+11)
			out[pos] = 0x0b
			pos++
			year, err := strconv.ParseUint(string(v.Raw[0:4]), 10, 16)
			if err != nil {
				return []byte{}, err
			}
			month, err := strconv.ParseUint(string(v.Raw[5:7]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			day, err := strconv.ParseUint(string(v.Raw[8:10]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			hour, err := strconv.ParseUint(string(v.Raw[11:13]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			minute, err := strconv.ParseUint(string(v.Raw[14:16]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			second, err := strconv.ParseUint(string(v.Raw[17:19]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			val := make([]byte, 6)
			count := copy(val, v.Raw[20:])
			for i := 0; i < (6 - count); i++ {
				val[count+i] = 0x30
			}
			microSecond, err := strconv.ParseUint(string(val), 10, 32)
			if err != nil {
				return []byte{}, err
			}
			pos = misc.WriteUint16(out, pos, uint16(year))
			pos = misc.WriteByte(out, pos, byte(month))
			pos = misc.WriteByte(out, pos, byte(day))
			pos = misc.WriteByte(out, pos, byte(hour))
			pos = misc.WriteByte(out, pos, byte(minute))
			pos = misc.WriteByte(out, pos, byte(second))
			misc.WriteUint32(out, pos, uint32(microSecond))
		} else if len(v.Raw) > 10 {
			out = make([]byte, 1+7)
			out[pos] = 0x07
			pos++
			year, err := strconv.ParseUint(string(v.Raw[0:4]), 10, 16)
			if err != nil {
				return []byte{}, err
			}
			month, err := strconv.ParseUint(string(v.Raw[5:7]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			day, err := strconv.ParseUint(string(v.Raw[8:10]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			hour, err := strconv.ParseUint(string(v.Raw[11:13]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			minute, err := strconv.ParseUint(string(v.Raw[14:16]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			second, err := strconv.ParseUint(string(v.Raw[17:]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			pos = misc.WriteUint16(out, pos, uint16(year))
			pos = misc.WriteByte(out, pos, byte(month))
			pos = misc.WriteByte(out, pos, byte(day))
			pos = misc.WriteByte(out, pos, byte(hour))
			pos = misc.WriteByte(out, pos, byte(minute))
			misc.WriteByte(out, pos, byte(second))
		} else if len(v.Raw) > 0 {
			out = make([]byte, 1+4)
			out[pos] = 0x04
			pos++
			year, err := strconv.ParseUint(string(v.Raw[0:4]), 10, 16)
			if err != nil {
				return []byte{}, err
			}
			month, err := strconv.ParseUint(string(v.Raw[5:7]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			day, err := strconv.ParseUint(string(v.Raw[8:]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			pos = misc.WriteUint16(out, pos, uint16(year))
			pos = misc.WriteByte(out, pos, byte(month))
			misc.WriteByte(out, pos, byte(day))
		} else {
			out = make([]byte, 1)
			out[pos] = 0x00
		}
	case constant.FieldTypeTime:
		if string(v.Raw) == "00:00:00" {
			out = make([]byte, 1)
			out[pos] = 0x00
		} else if strings.Contains(string(v.Raw), ".") {
			out = make([]byte, 1+12)
			out[pos] = 0x0c
			pos++

			sub1 := strings.Split(string(v.Raw), ":")
			if len(sub1) != 3 {
				err := fmt.Errorf("incorrect time value, ':' is not found")
				return []byte{}, err
			}
			sub2 := strings.Split(sub1[2], ".")
			if len(sub2) != 2 {
				err := fmt.Errorf("incorrect time value, '.' is not found")
				return []byte{}, err
			}

			var total []byte
			if strings.HasPrefix(sub1[0], "-") {
				out[pos] = 0x01
				total = []byte(sub1[0])
				total = total[1:]
			} else {
				out[pos] = 0x00
				total = []byte(sub1[0])
			}
			pos++

			h, err := strconv.ParseUint(string(total), 10, 32)
			if err != nil {
				return []byte{}, err
			}

			days := uint32(h) / 24
			hours := uint32(h) % 24
			minute := sub1[1]
			second := sub2[0]
			microSecond := sub2[1]

			minutes, err := strconv.ParseUint(minute, 10, 8)
			if err != nil {
				return []byte{}, err
			}

			seconds, err := strconv.ParseUint(second, 10, 8)
			if err != nil {
				return []byte{}, err
			}
			pos = misc.WriteUint32(out, pos, uint32(days))
			pos = misc.WriteByte(out, pos, byte(hours))
			pos = misc.WriteByte(out, pos, byte(minutes))
			pos = misc.WriteByte(out, pos, byte(seconds))

			val := make([]byte, 6)
			count := copy(val, microSecond)
			for i := 0; i < (6 - count); i++ {
				val[count+i] = 0x30
			}
			microSeconds, err := strconv.ParseUint(string(val), 10, 32)
			if err != nil {
				return []byte{}, err
			}
			misc.WriteUint32(out, pos, uint32(microSeconds))
		} else if len(v.Raw) > 0 {
			out = make([]byte, 1+8)
			out[pos] = 0x08
			pos++

			sub1 := strings.Split(string(v.Raw), ":")
			if len(sub1) != 3 {
				err := fmt.Errorf("incorrect time value, ':' is not found")
				return []byte{}, err
			}

			var total []byte
			if strings.HasPrefix(sub1[0], "-") {
				out[pos] = 0x01
				total = []byte(sub1[0])
				total = total[1:]
			} else {
				out[pos] = 0x00
				total = []byte(sub1[0])
			}
			pos++

			h, err := strconv.ParseUint(string(total), 10, 32)
			if err != nil {
				return []byte{}, err
			}

			days := uint32(h) / 24
			hours := uint32(h) % 24
			minute := sub1[1]
			second := sub1[2]

			minutes, err := strconv.ParseUint(minute, 10, 8)
			if err != nil {
				return []byte{}, err
			}

			seconds, err := strconv.ParseUint(second, 10, 8)
			if err != nil {
				return []byte{}, err
			}
			pos = misc.WriteUint32(out, pos, uint32(days))
			pos = misc.WriteByte(out, pos, byte(hours))
			pos = misc.WriteByte(out, pos, byte(minutes))
			misc.WriteByte(out, pos, byte(seconds))
		} else {
			err := fmt.Errorf("incorrect time value")
			return []byte{}, err
		}
	case constant.FieldTypeDecimal, constant.FieldTypeNewDecimal, constant.FieldTypeVarChar, constant.FieldTypeTinyBLOB,
		constant.FieldTypeMediumBLOB, constant.FieldTypeLongBLOB, constant.FieldTypeBLOB, constant.FieldTypeVarString,
		constant.FieldTypeString, constant.FieldTypeGeometry, constant.FieldTypeJSON, constant.FieldTypeBit,
		constant.FieldTypeEnum, constant.FieldTypeSet:
		l := len(v.Raw)
		length := misc.LenEncIntSize(uint64(l)) + l
		out = make([]byte, length)
		pos = misc.WriteLenEncInt(out, pos, uint64(l))
		copy(out[pos:], v.Raw)
	default:
		out = make([]byte, len(v.Raw))
		copy(out, v.Raw)
	}
	return out, nil
}

func BinaryVal2MySQL(v *proto.Value) ([]byte, error) {
	var out []byte
	pos := 0
	if v == nil {
		return out, nil
	}

	switch v.Typ {
	case constant.FieldTypeNULL:
		// no-op
	case constant.FieldTypeTiny:
		val := v.Val.(int64)
		out = make([]byte, 1)
		misc.WriteByte(out, pos, uint8(val))
	case constant.FieldTypeUint8:
		val := v.Val.(int64)
		out = make([]byte, 1)
		misc.WriteByte(out, pos, uint8(val))
	case constant.FieldTypeUint16:
		val := v.Val.(int64)
		out = make([]byte, 2)
		misc.WriteUint16(out, pos, uint16(val))
	case constant.FieldTypeShort, constant.FieldTypeYear:
		val := v.Val.(int64)
		out = make([]byte, 2)
		misc.WriteUint16(out, pos, uint16(val))
	case constant.FieldTypeUint24, constant.FieldTypeUint32:
		val := v.Val.(int64)
		out = make([]byte, 4)
		misc.WriteUint32(out, pos, uint32(val))
	case constant.FieldTypeInt24, constant.FieldTypeLong:
		val := v.Val.(int64)
		out = make([]byte, 4)
		misc.WriteUint32(out, pos, uint32(val))
	case constant.FieldTypeFloat:
		val := v.Val.(float32)
		bits := math.Float32bits(val)
		out = make([]byte, 4)
		misc.WriteUint32(out, pos, bits)
	case constant.FieldTypeUint64:
		if val, ok := v.Val.(int64); ok {
			out = make([]byte, 8)
			misc.WriteUint64(out, pos, uint64(val))
		} else {
			val, err := strconv.ParseUint(fmt.Sprintf("%s", v.Val), 10, 64)
			if err != nil {
				return []byte{}, err
			}
			out = make([]byte, 8)
			misc.WriteUint64(out, pos, val)
		}
	case constant.FieldTypeLongLong:
		val := v.Val.(int64)
		out = make([]byte, 8)
		misc.WriteUint64(out, pos, uint64(val))
	case constant.FieldTypeDouble:
		val := v.Val.(float64)
		bits := math.Float64bits(val)
		out = make([]byte, 8)
		misc.WriteUint64(out, pos, bits)
	case constant.FieldTypeTimestamp, constant.FieldTypeDate, constant.FieldTypeDateTime:
		if v.Val == nil {
			out = make([]byte, 1)
			out[pos] = 0x00
		}
		if _, ok := v.Val.([]byte); ok {
			out = make([]byte, 1+v.Len)
			out[pos] = byte(v.Len)
			pos++

			copy(out[pos:], v.Raw)
		}
		if val, ok := v.Val.(time.Time); ok {
			year := val.Year()
			month := val.Month()
			day := val.Day()
			hour := val.Hour()
			minute := val.Minute()
			second := val.Second()
			microSecond := val.Nanosecond() / int(time.Millisecond)

			if hour == 0 && minute == 0 && second == 0 && microSecond == 0 {
				out = make([]byte, 1+4)
				out[pos] = 0x04
				pos++

				pos = misc.WriteUint16(out, pos, uint16(year))
				pos = misc.WriteByte(out, pos, byte(month))
				misc.WriteByte(out, pos, byte(day))
			} else if microSecond == 0 {
				out = make([]byte, 1+7)
				out[pos] = 0x07
				pos++

				pos = misc.WriteUint16(out, pos, uint16(year))
				pos = misc.WriteByte(out, pos, byte(month))
				pos = misc.WriteByte(out, pos, byte(day))
				pos = misc.WriteByte(out, pos, byte(hour))
				pos = misc.WriteByte(out, pos, byte(minute))
				misc.WriteByte(out, pos, byte(second))
			} else {
				out = make([]byte, 1+11)
				out[pos] = 0x0b
				pos++

				pos = misc.WriteUint16(out, pos, uint16(year))
				pos = misc.WriteByte(out, pos, byte(month))
				pos = misc.WriteByte(out, pos, byte(day))
				pos = misc.WriteByte(out, pos, byte(hour))
				pos = misc.WriteByte(out, pos, byte(minute))
				pos = misc.WriteByte(out, pos, byte(second))
				misc.WriteUint32(out, pos, uint32(microSecond))
			}
		}
	case constant.FieldTypeTime:
		if v.Val == nil {
			out = make([]byte, 1)
			out[pos] = 0x00
		}
		if _, ok := v.Val.([]byte); ok {
			out = make([]byte, 1+v.Len)
			out[pos] = byte(v.Len)
			pos++

			copy(out[pos:], v.Raw)
		}
	case constant.FieldTypeDecimal, constant.FieldTypeNewDecimal, constant.FieldTypeVarChar, constant.FieldTypeTinyBLOB,
		constant.FieldTypeMediumBLOB, constant.FieldTypeLongBLOB, constant.FieldTypeBLOB, constant.FieldTypeVarString,
		constant.FieldTypeString, constant.FieldTypeGeometry, constant.FieldTypeJSON, constant.FieldTypeBit,
		constant.FieldTypeEnum, constant.FieldTypeSet:
		val := v.Val.([]byte)
		l := len(val)
		length := misc.LenEncIntSize(uint64(l)) + l
		out = make([]byte, length)
		pos = misc.WriteLenEncInt(out, pos, uint64(l))
		copy(out[pos:], val)
	default:
		out = make([]byte, len(v.Raw))
		copy(out, v.Raw)
	}
	return out, nil
}

func TextVal2MySQLLen(v *proto.Value) (int, error) {
	var length int
	var err error
	if v == nil {
		return 0, nil
	}

	switch v.Typ {
	case constant.FieldTypeNULL:
		length = 0
	case constant.FieldTypeTiny, constant.FieldTypeUint8:
		length = 1
	case constant.FieldTypeUint16, constant.FieldTypeShort, constant.FieldTypeYear:
		length = 2
	case constant.FieldTypeUint24, constant.FieldTypeUint32, constant.FieldTypeInt24, constant.FieldTypeLong, constant.FieldTypeFloat:
		length = 4
	case constant.FieldTypeUint64, constant.FieldTypeLongLong, constant.FieldTypeDouble:
		length = 8
	case constant.FieldTypeTimestamp, constant.FieldTypeDate, constant.FieldTypeDateTime:
		if len(v.Raw) > 19 {
			length = 12
		} else if len(v.Raw) > 10 {
			length = 8
		} else if len(v.Raw) > 0 {
			length = 5
		} else {
			length = 1
		}
	case constant.FieldTypeTime:
		if string(v.Raw) == "00:00:00" {
			length = 1
		} else if strings.Contains(string(v.Raw), ".") {
			length = 13
		} else if len(v.Raw) > 0 {
			length = 9
		} else {
			err = fmt.Errorf("incorrect time value")
		}
	case constant.FieldTypeDecimal, constant.FieldTypeNewDecimal, constant.FieldTypeVarChar, constant.FieldTypeTinyBLOB,
		constant.FieldTypeMediumBLOB, constant.FieldTypeLongBLOB, constant.FieldTypeBLOB, constant.FieldTypeVarString,
		constant.FieldTypeString, constant.FieldTypeGeometry, constant.FieldTypeJSON, constant.FieldTypeBit,
		constant.FieldTypeEnum, constant.FieldTypeSet:
		l := len(v.Raw)
		length = misc.LenEncIntSize(uint64(l)) + l
	default:
		length = len(v.Raw)
	}
	if err != nil {
		return 0, err
	}
	return length, nil
}

func BinaryVal2MySQLLen(v *proto.Value) (int, error) {
	var length int
	var err error
	if v == nil {
		return 0, nil
	}

	switch v.Typ {
	case constant.FieldTypeNULL:
		length = 0
	case constant.FieldTypeTiny, constant.FieldTypeUint8:
		length = 1
	case constant.FieldTypeUint16, constant.FieldTypeShort, constant.FieldTypeYear:
		length = 2
	case constant.FieldTypeUint24, constant.FieldTypeUint32, constant.FieldTypeInt24, constant.FieldTypeLong, constant.FieldTypeFloat:
		length = 4
	case constant.FieldTypeUint64, constant.FieldTypeLongLong, constant.FieldTypeDouble:
		length = 8
	case constant.FieldTypeTimestamp, constant.FieldTypeDate, constant.FieldTypeDateTime:
		if v.Val == nil {
			length = 1
		}
		if _, ok := v.Val.([]byte); ok {
			length = v.Len + 1
		}
		if val, ok := v.Val.(time.Time); ok {
			if val.Hour() == 0 && val.Minute() == 0 &&
				val.Second() == 0 && val.Nanosecond() == 0 {
				length = 5
			} else if val.Hour() == 0 && val.Minute() == 0 &&
				val.Second() == 0 {
				length = 8
			} else {
				length = 12
			}
		}
	case constant.FieldTypeTime:
		if v.Val == nil {
			length = 1
		}
		if _, ok := v.Val.([]byte); ok {
			length = v.Len + 1
		}
	case constant.FieldTypeDecimal, constant.FieldTypeNewDecimal, constant.FieldTypeVarChar, constant.FieldTypeTinyBLOB,
		constant.FieldTypeMediumBLOB, constant.FieldTypeLongBLOB, constant.FieldTypeBLOB, constant.FieldTypeVarString,
		constant.FieldTypeString, constant.FieldTypeGeometry, constant.FieldTypeJSON, constant.FieldTypeBit,
		constant.FieldTypeEnum, constant.FieldTypeSet:
		val := v.Val.([]byte)
		l := len(val)
		length = misc.LenEncIntSize(uint64(l)) + l
	default:
		length = len(v.Raw)
	}
	if err != nil {
		return 0, err
	}
	return length, nil
}

//
// Packet parsing methods, for generic packets.
//
// IsEOFPacket determines whether or not a Content packet is a "true" EOF. DO NOT blindly compare the
// first byte of a packet to EOFPacket as you might do for other packet types, as 0xfe is overloaded
// as a first byte.
//
// Per https://dev.mysql.com/doc/internals/en/packet-EOF_Packet.html, a packet starting with 0xfe
// but having length >= 9 (on top of 4 byte header) is not a true EOF but a LengthEncodedInteger
// (typically preceding a LengthEncodedString). Thus, all EOF checks must validate the payload size
// before exiting.
//
// More specifically, an EOF packet can have 3 different lengths (1, 5, 7) depending on the client
// flags that are set. 7 comes from server versions of 5.7.5 or greater where ClientDeprecateEOF is
// set (i.e. uses an OK packet starting with 0xfe instead of 0x00 to signal EOF). Regardless, 8 is
// an upper bound otherwise it would be ambiguous w.r.t. LengthEncodedIntegers.
//
// More docs here:
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_response_packets.html
func IsEOFPacket(data []byte) bool {
	return data[0] == constant.EOFPacket && len(data) < 9
}

// ParseEOFPacket returns the warning count and a boolean to indicate if there
// are more results to receive.
//
// Note: This is only valid on actual EOF packets and not on OK packets with the EOF
// type code set, i.e. should not be used if ClientDeprecateEOF is set.
func ParseEOFPacket(data []byte) (warnings uint16, more bool, err error) {
	// The warning count is in position 2 & 3
	warnings, _, _ = misc.ReadUint16(data, 1)

	// The status flag is in position 4 & 5
	statusFlags, _, ok := misc.ReadUint16(data, 3)
	if !ok {
		return 0, false, errors.Errorf("invalid EOF packet statusFlags: %v", data)
	}
	return warnings, (statusFlags & mysql.ServerMoreResultsExists) != 0, nil
}

func ParseOKPacket(data []byte) (uint64, uint64, uint16, uint16, error) {
	// We already read the type.
	pos := 1

	// Affected rows.
	affectedRows, pos, ok := misc.ReadLenEncInt(data, pos)
	if !ok {
		return 0, 0, 0, 0, errors.Errorf("invalid OK packet affectedRows: %v", data)
	}

	// Last Insert ID.
	lastInsertID, pos, ok := misc.ReadLenEncInt(data, pos)
	if !ok {
		return 0, 0, 0, 0, errors.Errorf("invalid OK packet lastInsertID: %v", data)
	}

	// Status flags.
	statusFlags, pos, ok := misc.ReadUint16(data, pos)
	if !ok {
		return 0, 0, 0, 0, errors.Errorf("invalid OK packet statusFlags: %v", data)
	}

	// Warnings.
	warnings, _, ok := misc.ReadUint16(data, pos)
	if !ok {
		return 0, 0, 0, 0, errors.Errorf("invalid OK packet warnings: %v", data)
	}

	return affectedRows, lastInsertID, statusFlags, warnings, nil
}

// IsErrorPacket determines whether or not the packet is an error packet. Mostly here for
// consistency with isEOFPacket
func IsErrorPacket(data []byte) bool {
	return data[0] == constant.ErrPacket
}

// ParseErrorPacket parses the error packet and returns a SQLError.
func ParseErrorPacket(data []byte) error {
	// We already read the type.
	pos := 1

	// Error code is 2 bytes.
	code, pos, ok := misc.ReadUint16(data, pos)
	if !ok {
		return err2.NewSQLError(constant.CRUnknownError, constant.SSUnknownSQLState, "invalid error packet code: %v", data)
	}

	// '#' marker of the SQL state is 1 byte. Ignored.
	pos++

	// SQL state is 5 bytes
	sqlState, pos, ok := misc.ReadBytes(data, pos, 5)
	if !ok {
		return err2.NewSQLError(constant.CRUnknownError, constant.SSUnknownSQLState, "invalid error packet sqlState: %v", data)
	}

	// Human readable error message is the rest.
	msg := string(data[pos:])

	return err2.NewSQLError(int(code), string(sqlState), "%v", msg)
}
