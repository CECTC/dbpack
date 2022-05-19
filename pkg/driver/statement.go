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

package driver

import (
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/cectc/dbpack/pkg/constant"
	"github.com/cectc/dbpack/pkg/errors"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/misc"
	"github.com/cectc/dbpack/pkg/mysql"
	"github.com/cectc/dbpack/pkg/packet"
)

type BackendStatement struct {
	conn       *BackendConnection
	id         uint32
	paramCount int
	sql        string
}

// prepare Result Packets
// http://dev.mysql.com/doc/internals/en/com-stmt-prepare-response.html
func (stmt *BackendStatement) readPrepareResultPacket() (uint16, error) {
	data, err := stmt.conn.ReadEphemeralPacket()
	if err != nil {
		return 0, errors.NewSQLError(constant.CRServerLost, constant.SSUnknownSQLState, "%v", err)
	}
	defer stmt.conn.RecycleReadPacket()

	// packet indicator [1 byte]
	if data[0] != constant.OKPacket {
		return 0, packet.ParseErrorPacket(data)
	}

	// statement id [4 bytes]
	stmt.id = binary.LittleEndian.Uint32(data[1:5])

	// Column count [16 bit uint]
	columnCount := binary.LittleEndian.Uint16(data[5:7])

	// Param count [16 bit uint]
	stmt.paramCount = int(binary.LittleEndian.Uint16(data[7:9]))

	// Reserved [8 bit]

	// Warning count [16 bit uint]

	return columnCount, nil
}

// http://dev.mysql.com/doc/internals/en/com-stmt-send-long-data.html
func (stmt *BackendStatement) writeCommandLongData(paramID int, arg []byte) error {
	maxLen := stmt.conn.conf.MaxAllowedPacket - 1
	pktLen := maxLen

	// After the header (bytes 0-3) follows before the data:
	// 1 byte command
	// 4 bytes stmtID
	// 2 bytes paramID
	const dataOffset = 1 + 4 + 2

	// Cannot use the write buffer since
	// a) the buffer is too small
	// b) it is in use
	data := make([]byte, 4+1+4+2+len(arg))

	copy(data[4+dataOffset:], arg)

	for argLen := len(arg); argLen > 0; argLen -= pktLen - dataOffset {
		if dataOffset+argLen < maxLen {
			pktLen = dataOffset + argLen
		}

		stmt.conn.ResetSequence()
		// Add command byte [1 byte]
		data[4] = constant.ComStmtSendLongData

		// Add stmtID [32 bit]
		data[5] = byte(stmt.id)
		data[6] = byte(stmt.id >> 8)
		data[7] = byte(stmt.id >> 16)
		data[8] = byte(stmt.id >> 24)

		// Add paramID [16 bit]
		data[9] = byte(paramID)
		data[10] = byte(paramID >> 8)

		// Send CMD packet
		err := stmt.conn.WritePacket(data[:4+pktLen])
		if err == nil {
			data = data[pktLen-dataOffset:]
			continue
		}
		return err

	}

	// Reset Packet Sequence
	stmt.conn.ResetSequence()
	return nil
}

// Execute Prepared Statement
// http://dev.mysql.com/doc/internals/en/com-stmt-execute.html
func (stmt *BackendStatement) writeExecutePacket(args []interface{}) error {
	if len(args) != stmt.paramCount {
		return fmt.Errorf(
			"argument count mismatch (got: %d; has: %d)",
			len(args),
			stmt.paramCount,
		)
	}

	const minPktLen = 4 + 1 + 4 + 1 + 4
	bc := stmt.conn

	// Determine threshold dynamically to avoid packet size shortage.
	longDataSize := bc.conf.MaxAllowedPacket / (stmt.paramCount + 1)
	if longDataSize < 64 {
		longDataSize = 64
	}

	// Reset packet-sequence
	bc.ResetSequence()

	var data []byte
	var err error

	if len(args) == 0 {
		data = stmt.conn.StartEphemeralPacket(minPktLen)
	} else {
		data = stmt.conn.StartEphemeralPacket(constant.MaxPacketSize)
		// In this case the len(data) == cap(data) which is used to optimize the flow below.
	}
	defer bc.RecycleWritePacket()

	// command [1 byte]
	data[4] = constant.ComStmtExecute

	// statement_id [4 bytes]
	data[5] = byte(stmt.id)
	data[6] = byte(stmt.id >> 8)
	data[7] = byte(stmt.id >> 16)
	data[8] = byte(stmt.id >> 24)

	// flags (0: CURSOR_TYPE_NO_CURSOR) [1 byte]
	data[9] = 0x00

	// iteration_count (uint32(1)) [4 bytes]
	data[10] = 0x01
	data[11] = 0x00
	data[12] = 0x00
	data[13] = 0x00

	if len(args) > 0 {
		pos := minPktLen

		var nullMask []byte
		if maskLen, typesLen := (len(args)+7)/8, 1+2*len(args); pos+maskLen+typesLen >= cap(data) {
			// buffer has to be extended but we don't know by how much so
			// we depend on append after all data with known sizes fit.
			// We stop at that because we deal with a lot of columns here
			// which makes the required allocation size hard to guess.
			tmp := make([]byte, pos+maskLen+typesLen)
			copy(tmp[:pos], data[:pos])
			data = tmp
			nullMask = data[pos : pos+maskLen]
			// No need to clean nullMask as make ensures that.
			pos += maskLen
		} else {
			nullMask = data[pos : pos+maskLen]
			for i := range nullMask {
				nullMask[i] = 0
			}
			pos += maskLen
		}

		// newParameterBoundFlag 1 [1 byte]
		data[pos] = 0x01
		pos++

		// type of each parameter [len(args)*2 bytes]
		paramTypes := data[pos:]
		pos += len(args) * 2

		// value of each parameter [n bytes]
		paramValues := data[pos:pos]
		valuesCap := cap(paramValues)

		for i, arg := range args {
			// build NULL-bitmap
			if arg == nil {
				nullMask[i/8] |= 1 << (uint(i) & 7)
				paramTypes[i+i] = byte(constant.FieldTypeNULL)
				paramTypes[i+i+1] = 0x00
				continue
			}

			if v, ok := arg.(json.RawMessage); ok {
				arg = []byte(v)
			}
			// cache types and values
			switch v := arg.(type) {
			case int64:
				paramTypes[i+i] = byte(constant.FieldTypeLongLong)
				paramTypes[i+i+1] = 0x00

				if cap(paramValues)-len(paramValues)-8 >= 0 {
					paramValues = paramValues[:len(paramValues)+8]
					binary.LittleEndian.PutUint64(
						paramValues[len(paramValues)-8:],
						uint64(v),
					)
				} else {
					paramValues = append(paramValues,
						misc.Uint64ToBytes(uint64(v))...,
					)
				}

			case uint64:
				paramTypes[i+i] = byte(constant.FieldTypeLongLong)
				paramTypes[i+i+1] = 0x80 // type is unsigned

				if cap(paramValues)-len(paramValues)-8 >= 0 {
					paramValues = paramValues[:len(paramValues)+8]
					binary.LittleEndian.PutUint64(
						paramValues[len(paramValues)-8:],
						uint64(v),
					)
				} else {
					paramValues = append(paramValues,
						misc.Uint64ToBytes(uint64(v))...,
					)
				}

			case float64:
				paramTypes[i+i] = byte(constant.FieldTypeDouble)
				paramTypes[i+i+1] = 0x00

				if cap(paramValues)-len(paramValues)-8 >= 0 {
					paramValues = paramValues[:len(paramValues)+8]
					binary.LittleEndian.PutUint64(
						paramValues[len(paramValues)-8:],
						math.Float64bits(v),
					)
				} else {
					paramValues = append(paramValues,
						misc.Uint64ToBytes(math.Float64bits(v))...,
					)
				}

			case bool:
				paramTypes[i+i] = byte(constant.FieldTypeTiny)
				paramTypes[i+i+1] = 0x00

				if v {
					paramValues = append(paramValues, 0x01)
				} else {
					paramValues = append(paramValues, 0x00)
				}

			case []byte:
				// Common case (non-nil value) first
				if v != nil {
					paramTypes[i+i] = byte(constant.FieldTypeString)
					paramTypes[i+i+1] = 0x00

					if len(v) < longDataSize {
						paramValues = misc.AppendLengthEncodedInteger(paramValues,
							uint64(len(v)),
						)
						paramValues = append(paramValues, v...)
					} else {
						if err := stmt.writeCommandLongData(i, v); err != nil {
							return err
						}
					}
					continue
				}

				// Handle []byte(nil) as a NULL value
				nullMask[i/8] |= 1 << (uint(i) & 7)
				paramTypes[i+i] = byte(constant.FieldTypeNULL)
				paramTypes[i+i+1] = 0x00

			case string:
				paramTypes[i+i] = byte(constant.FieldTypeString)
				paramTypes[i+i+1] = 0x00

				if len(v) < longDataSize {
					paramValues = misc.AppendLengthEncodedInteger(paramValues,
						uint64(len(v)),
					)
					paramValues = append(paramValues, v...)
				} else {
					if err := stmt.writeCommandLongData(i, []byte(v)); err != nil {
						return err
					}
				}

			case time.Time:
				paramTypes[i+i] = byte(constant.FieldTypeString)
				paramTypes[i+i+1] = 0x00

				var a [64]byte
				var b = a[:0]

				if v.IsZero() {
					b = append(b, "0000-00-00"...)
				} else {
					b, err = misc.AppendDateTime(b, v.In(bc.conf.Loc))
					if err != nil {
						return err
					}
				}

				paramValues = misc.AppendLengthEncodedInteger(paramValues,
					uint64(len(b)),
				)
				paramValues = append(paramValues, b...)

			default:
				return fmt.Errorf("cannot convert type: %T", arg)
			}
		}

		// Check if param values exceeded the available buffer
		// In that case we must build the data packet with the new values buffer
		if valuesCap != cap(paramValues) {
			data = append(data[:pos], paramValues...)
		}

		pos += len(paramValues)
		data = data[:pos]
	}

	return bc.WritePacket(data[4:])
}

func (stmt *BackendStatement) execArgs(args []interface{}) (*mysql.Result, uint16, error) {
	nargs := make([]interface{}, len(args))
	for i, arg := range args {
		var err error
		nargs[i], err = driver.DefaultParameterConverter.ConvertValue(arg)
		if err != nil {
			return nil, 0, fmt.Errorf("sql: converting argument %t type: %v", arg, err)
		}
	}

	err := stmt.writeExecutePacket(nargs)
	if err != nil {
		return nil, 0, err
	}

	affectedRows, lastInsertID, colNumber, _, warnings, err := stmt.conn.ReadComQueryResponse()
	if err != nil {
		return nil, 0, err
	}

	if colNumber > 0 {
		// columns
		if err = stmt.conn.DrainResults(); err != nil {
			return nil, 0, err
		}
		// rows
		if err = stmt.conn.DrainResults(); err != nil {
			return nil, 0, err
		}
	}

	return &mysql.Result{
		AffectedRows: affectedRows,
		InsertId:     lastInsertID,
	}, warnings, nil
}

func (stmt *BackendStatement) queryArgs(args []interface{}) (*mysql.Result, uint16, error) {
	nargs := make([]interface{}, len(args))
	for i, arg := range args {
		var err error
		nargs[i], err = driver.DefaultParameterConverter.ConvertValue(arg)
		if err != nil {
			return nil, 0, fmt.Errorf("sql: converting argument %t type: %v", arg, err)
		}
	}

	err := stmt.writeExecutePacket(nargs)
	if err != nil {
		return nil, 0, err
	}

	result, _, warnings, err := stmt.conn.ReadQueryResult(true)
	return result, warnings, err
}

func (stmt *BackendStatement) exec(args []byte) (*mysql.Result, uint16, error) {
	defer func() {
		if err := stmt.conn.WriteComStmtClose(stmt.id); err != nil {
			log.Error(err)
		}
	}()

	args[1] = byte(stmt.id)
	args[2] = byte(stmt.id >> 8)
	args[3] = byte(stmt.id >> 16)
	args[4] = byte(stmt.id >> 24)

	// Reset packet-sequence
	stmt.conn.ResetSequence()

	err := stmt.conn.WritePacket(args)
	if err != nil {
		return nil, 0, err
	}
	stmt.conn.RecycleWritePacket()

	affectedRows, lastInsertID, colNumber, _, warnings, err := stmt.conn.ReadComQueryResponse()
	if err != nil {
		return nil, 0, err
	}

	if colNumber > 0 {
		// columns
		if err = stmt.conn.DrainResults(); err != nil {
			return nil, 0, err
		}
		// rows
		if err = stmt.conn.DrainResults(); err != nil {
			return nil, 0, err
		}
	}

	return &mysql.Result{
		AffectedRows: affectedRows,
		InsertId:     lastInsertID,
	}, warnings, nil
}

func (stmt *BackendStatement) query(args []byte) (*mysql.Result, uint16, error) {
	args[1] = byte(stmt.id)
	args[2] = byte(stmt.id >> 8)
	args[3] = byte(stmt.id >> 16)
	args[4] = byte(stmt.id >> 24)

	// Reset packet-sequence
	stmt.conn.ResetSequence()

	err := stmt.conn.WritePacket(args)
	if err != nil {
		return nil, 0, err
	}

	result, _, warnings, err := stmt.conn.ReadQueryResult(true)
	return result, warnings, err
}
