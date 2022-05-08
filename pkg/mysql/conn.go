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
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/cectc/dbpack/pkg/constant"
	err2 "github.com/cectc/dbpack/pkg/errors"
	"github.com/cectc/dbpack/pkg/misc"
	"github.com/cectc/dbpack/pkg/packet"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/third_party/bucketpool"
	"github.com/cectc/dbpack/third_party/sync2"
)

const (
	// connBufferSize is how much we buffer for reading and
	// writing. It is also how much we allocate for ephemeral buffers.
	connBufferSize = 16 * 1024
)

// Constants for how ephemeral buffers were used for reading / writing.
const (
	// ephemeralUnused means the ephemeral buffer is not in use at this
	// moment. This is the default value, and is checked so we don't
	// read or write a packet while one is already used.
	ephemeralUnused = iota

	// ephemeralWrite means we currently in process of writing from  currentEphemeralBuffer
	ephemeralWrite

	// ephemeralRead means we currently in process of reading into currentEphemeralBuffer
	ephemeralRead
)

var mysqlServerFlushDelay = flag.Duration("mysql_server_flush_delay", 100*time.Millisecond, "Delay after which buffered response will flushed to client.")

// bufPool is used to allocate and free buffers in an efficient way.
var bufPool = bucketpool.New(connBufferSize, constant.MaxPacketSize)

// writersPool is used for pooling bufio.Writer objects.
var writersPool = sync.Pool{New: func() interface{} { return bufio.NewWriterSize(nil, connBufferSize) }}

// Conn is a connection between a client and a server, using the MySQL
// binary protocol. It is built on top of an existing net.Conn, that
// has already been established.
//
// Use Connect on the client side to create a connection.
// Use NewListener to create a server side and listen for connections.
type Conn struct {
	// conn is the underlying network connection.
	// Calling Close() on the Conn will close this connection.
	// If there are any ongoing reads or writes, they may get interrupted.
	conn net.Conn

	// ConnectionID is set:
	// - at Connect() time for clients, with the value returned by
	// the server.
	// - at accept time for the server.
	ConnectionID uint32

	// closed is set to true when Close() is called on the connection.
	closed sync2.AtomicBool

	// StatusFlags are the status flags we will base our returned flags on.
	// This is a bit field, with values documented in constants.go.
	// An interesting value here would be ServerStatusAutocommit.
	// It is only used by the server. These flags can be changed
	// by Handler methods.
	StatusFlags uint16

	// Packet encoding variables.
	sequence       uint8
	bufferedReader *bufio.Reader

	// Buffered writing has a timer which flushes on inactivity.
	bufMu          sync.Mutex
	bufferedWriter *bufio.Writer
	flushTimer     *time.Timer

	// Keep track of how and of the buffer we allocated for an
	// ephemeral packet on the read and write sides.
	// These fields are used by:
	// - StartEphemeralPacket / WriteEphemeralPacket methods for writes.
	// - ReadEphemeralPacket / RecycleReadPacket methods for reads.
	currentEphemeralPolicy int
	// currentEphemeralBuffer for tracking allocated temporary buffer for writes and reads respectively.
	// It can be allocated from bufPool or heap and should be recycled in the same manner.
	currentEphemeralBuffer *[]byte
}

// NewConn is an internal method to create a Conn. Used by client and server
// side for common creation code.
func NewConn(conn net.Conn) *Conn {
	return &Conn{
		conn:           conn,
		closed:         sync2.NewAtomicBool(false),
		bufferedReader: bufio.NewReaderSize(conn, connBufferSize),
	}
}

// StartWriterBuffering starts using buffered writes. This should
// be terminated by a call to endWriteBuffering.
func (c *Conn) StartWriterBuffering() {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()

	c.bufferedWriter = writersPool.Get().(*bufio.Writer)
	c.bufferedWriter.Reset(c.conn)
}

// EndWriterBuffering must be called to terminate startWriteBuffering.
func (c *Conn) EndWriterBuffering() error {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()

	if c.bufferedWriter == nil {
		return nil
	}

	defer func() {
		c.bufferedWriter.Reset(nil)
		writersPool.Put(c.bufferedWriter)
		c.bufferedWriter = nil
	}()

	c.stopFlushTimer()
	return c.bufferedWriter.Flush()
}

// getWriter returns the current writer. It may be either
// the original connection or a wrapper. The returned unget
// function must be invoked after the writing is finished.
// In buffered mode, the unget starts a timer to flush any
// buffered Content.
func (c *Conn) getWriter() (w io.Writer, unget func()) {
	c.bufMu.Lock()
	if c.bufferedWriter != nil {
		return c.bufferedWriter, func() {
			c.startFlushTimer()
			c.bufMu.Unlock()
		}
	}
	c.bufMu.Unlock()
	return c.conn, func() {}
}

// startFlushTimer must be called while holding lock on bufMu.
func (c *Conn) startFlushTimer() {
	c.stopFlushTimer()
	c.flushTimer = time.AfterFunc(*mysqlServerFlushDelay, func() {
		c.bufMu.Lock()
		defer c.bufMu.Unlock()

		if c.bufferedWriter == nil {
			return
		}
		c.stopFlushTimer()
		c.bufferedWriter.Flush()
	})
}

// stopFlushTimer must be called while holding lock on bufMu.
func (c *Conn) stopFlushTimer() {
	if c.flushTimer != nil {
		c.flushTimer.Stop()
		c.flushTimer = nil
	}
}

func (c *Conn) ResetSequence() {
	c.sequence = 0
}

// getReader returns reader for connection. It can be *bufio.Reader or net.Conn
// depending on which buffer size was passed to newServerConn.
func (c *Conn) getReader() io.Reader {
	if c.bufferedReader != nil {
		return c.bufferedReader
	}
	return c.conn
}

func (c *Conn) readHeaderFrom(r io.Reader) (int, error) {
	var header [4]byte
	// Note io.ReadFull will return two different types of errors:
	// 1. if the socket is already closed, and the go runtime knows it,
	//   then ReadFull will return an error (different than EOF),
	//   something like 'read: connection reset by peer'.
	// 2. if the socket is not closed while we start the read,
	//   but gets closed after the read is started, we'll get io.EOF.
	if _, err := io.ReadFull(r, header[:]); err != nil {
		// The special casing of propagating io.EOF up
		// is used by the server side only, to suppress an error
		// message if a client just disconnects.
		if err == io.EOF {
			return 0, err
		}
		if strings.HasSuffix(err.Error(), "read: connection reset by peer") {
			return 0, io.EOF
		}
		return 0, errors.Wrapf(err, "io.ReadFull(header size) failed")
	}

	sequence := uint8(header[3])
	if sequence != c.sequence {
		return 0, errors.Errorf("invalid Sequence, expected %v got %v", c.sequence, sequence)
	}

	c.sequence++

	return int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16), nil
}

// ReadEphemeralPacket attempts to read a packet into buffer.  Do
// not use this method if the contents of the packet needs to be kept
// after the next ReadEphemeralPacket.
//
// Note if the connection is closed already, an error will be
// returned, and it may not be io.EOF. If the connection closes while
// we are stuck waiting for Content, an error will also be returned, and
// it most likely will be io.EOF.
func (c *Conn) ReadEphemeralPacket() ([]byte, error) {
	if c.currentEphemeralPolicy != ephemeralUnused {
		panic(errors.Errorf("ReadEphemeralPacket: unexpected currentEphemeralPolicy: %v", c.currentEphemeralPolicy))
	}

	r := c.getReader()

	length, err := c.readHeaderFrom(r)
	if err != nil {
		return nil, err
	}

	c.currentEphemeralPolicy = ephemeralRead
	if length == 0 {
		// This can be caused by the packet after a packet of
		// exactly size MaxPacketSize.
		return nil, nil
	}

	// Use the bufPool.
	if length < constant.MaxPacketSize {
		c.currentEphemeralBuffer = bufPool.Get(length)
		if _, err := io.ReadFull(r, *c.currentEphemeralBuffer); err != nil {
			return nil, errors.Wrapf(err, "io.ReadFull(packet body of length %v) failed", length)
		}
		return *c.currentEphemeralBuffer, nil
	}

	// Much slower path, revert to allocating everything from scratch.
	// We're going to concatenate a lot of Content anyway, can't really
	// optimize this code path easily.
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, errors.Wrapf(err, "io.ReadFull(packet body of length %v) failed", length)
	}
	for {
		next, err := c.ReadOnePacket()
		if err != nil {
			return nil, err
		}

		if len(next) == 0 {
			// Again, the packet after a packet of exactly size MaxPacketSize.
			break
		}

		data = append(data, next...)
		if len(next) < constant.MaxPacketSize {
			break
		}
	}

	return data, nil
}

// ReadEphemeralPacketDirect attempts to read a packet from the socket directly.
// It needs to be used for the first handshake packet the server receives,
// so we do't buffer the SSL negotiation packet. As a shortcut, only
// packets smaller than MaxPacketSize can be read here.
// This function usually shouldn't be used - use ReadEphemeralPacket.
func (c *Conn) ReadEphemeralPacketDirect() ([]byte, error) {
	if c.currentEphemeralPolicy != ephemeralUnused {
		panic(errors.Errorf("ReadEphemeralPacketDirect: unexpected currentEphemeralPolicy: %v", c.currentEphemeralPolicy))
	}

	var r io.Reader = c.conn

	length, err := c.readHeaderFrom(r)
	if err != nil {
		return nil, err
	}

	c.currentEphemeralPolicy = ephemeralRead
	if length == 0 {
		// This can be caused by the packet after a packet of
		// exactly size MaxPacketSize.
		return nil, nil
	}

	if length < constant.MaxPacketSize {
		c.currentEphemeralBuffer = bufPool.Get(length)
		if _, err := io.ReadFull(r, *c.currentEphemeralBuffer); err != nil {
			return nil, errors.Wrapf(err, "io.ReadFull(packet body of length %v) failed", length)
		}
		return *c.currentEphemeralBuffer, nil
	}

	return nil, errors.Errorf("ReadEphemeralPacketDirect doesn't support more than one packet")
}

// RecycleReadPacket recycles the read packet. It needs to be called
// after ReadEphemeralPacket was called.
func (c *Conn) RecycleReadPacket() {
	if c.currentEphemeralPolicy != ephemeralRead {
		// Programming error.
		panic(errors.Errorf("trying to call RecycleReadPacket while currentEphemeralPolicy is %d", c.currentEphemeralPolicy))
	}
	if c.currentEphemeralBuffer != nil {
		// We are using the pool, put the buffer back in.
		bufPool.Put(c.currentEphemeralBuffer)
		c.currentEphemeralBuffer = nil
	}
	c.currentEphemeralPolicy = ephemeralUnused
}

// ReadOnePacket reads a single packet into a newly allocated buffer.
func (c *Conn) ReadOnePacket() ([]byte, error) {
	r := c.getReader()
	length, err := c.readHeaderFrom(r)
	if err != nil {
		return nil, err
	}
	if length == 0 {
		// This can be caused by the packet after a packet of
		// exactly size MaxPacketSize.
		return nil, nil
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, errors.Wrapf(err, "io.ReadFull(packet body of length %v) failed", length)
	}
	return data, nil
}

// ReadPacket reads a packet from the underlying connection.
// It re-assembles packets that span more than one message.
// This method returns a generic error, not a SQLError.
func (c *Conn) ReadPacket() ([]byte, error) {
	// Optimize for a single packet case.
	data, err := c.ReadOnePacket()
	if err != nil {
		return nil, err
	}

	// This is a single packet.
	if len(data) < constant.MaxPacketSize {
		return data, nil
	}

	// There is more than one packet, read them all.
	for {
		next, err := c.ReadOnePacket()
		if err != nil {
			return nil, err
		}

		if len(next) == 0 {
			// Again, the packet after a packet of exactly size MaxPacketSize.
			break
		}

		data = append(data, next...)
		if len(next) < constant.MaxPacketSize {
			break
		}
	}

	return data, nil
}

// ReadPacketFacade reads a packet from the underlying connection.
// it is the public API version, that returns a SQLError.
// The memory for the packet is always allocated, and it is owned by the caller
// after this function returns.
func (c *Conn) ReadPacketFacade() ([]byte, error) {
	result, err := c.ReadPacket()
	if err != nil {
		return nil, err2.NewSQLError(constant.CRServerLost, constant.SSUnknownSQLState, "%v", err)
	}
	return result, err
}

// WritePacket writes a packet, possibly cutting it into multiple
// chunks.  Note this is not very efficient, as the client probably
// has to build the []byte and that makes a memory copy.
// Try to use StartEphemeralPacket/WriteEphemeralPacket instead.
//
// This method returns a generic error, not a SQLError.
func (c *Conn) WritePacket(data []byte) error {
	index := 0
	length := len(data)

	w, unget := c.getWriter()
	defer unget()

	for {
		// Packet length is capped to MaxPacketSize.
		packetLength := length
		if packetLength > constant.MaxPacketSize {
			packetLength = constant.MaxPacketSize
		}

		// Compute and write the header.
		var header [4]byte
		header[0] = byte(packetLength)
		header[1] = byte(packetLength >> 8)
		header[2] = byte(packetLength >> 16)
		header[3] = c.sequence
		if n, err := w.Write(header[:]); err != nil {
			return errors.Wrapf(err, "Write(header) failed")
		} else if n != 4 {
			return errors.Errorf("Write(header) returned a short write: %v < 4", n)
		}

		// Write the body.
		if n, err := w.Write(data[index : index+packetLength]); err != nil {
			return errors.Wrapf(err, "Write(packet) failed")
		} else if n != packetLength {
			return errors.Errorf("Write(packet) returned a short write: %v < %v", n, packetLength)
		}

		// Update our state.
		c.sequence++
		length -= packetLength
		if length == 0 {
			if packetLength == constant.MaxPacketSize {
				// The packet we just sent had exactly
				// MaxPacketSize size, we need to
				// sent a zero-size packet too.
				header[0] = 0
				header[1] = 0
				header[2] = 0
				header[3] = c.sequence
				if n, err := w.Write(header[:]); err != nil {
					return errors.Wrapf(err, "Write(empty header) failed")
				} else if n != 4 {
					return errors.Errorf("Write(empty header) returned a short write: %v < 4", n)
				}
				c.sequence++
			}
			return nil
		}
		index += packetLength
	}
}

func (c *Conn) StartEphemeralPacket(length int) []byte {
	if c.currentEphemeralPolicy != ephemeralUnused {
		panic("StartEphemeralPacket cannot be used while a packet is already started.")
	}

	c.currentEphemeralPolicy = ephemeralWrite
	// get buffer from pool or it'll be allocated if length is too big
	c.currentEphemeralBuffer = bufPool.Get(length)
	return *c.currentEphemeralBuffer
}

// WriteEphemeralPacket writes the packet that was allocated by
// StartEphemeralPacket.
func (c *Conn) WriteEphemeralPacket() error {
	defer c.RecycleWritePacket()

	switch c.currentEphemeralPolicy {
	case ephemeralWrite:
		if err := c.WritePacket(*c.currentEphemeralBuffer); err != nil {
			return errors.Wrapf(err, "conn %v", c.ID())
		}
	case ephemeralUnused, ephemeralRead:
		// Programming error.
		panic(errors.Errorf("conn %v: trying to call WriteEphemeralPacket while currentEphemeralPolicy is %v", c.ID(), c.currentEphemeralPolicy))
	}

	return nil
}

// RecycleWritePacket recycles the write packet. It needs to be called
// after WriteEphemeralPacket was called.
func (c *Conn) RecycleWritePacket() {
	if c.currentEphemeralPolicy != ephemeralWrite {
		// Programming error.
		panic(errors.Errorf("trying to call recycleWritePacket while currentEphemeralPolicy is %d", c.currentEphemeralPolicy))
	}
	// Release our reference so the buffer can be gced
	bufPool.Put(c.currentEphemeralBuffer)
	c.currentEphemeralBuffer = nil
	c.currentEphemeralPolicy = ephemeralUnused
}

func (c *Conn) sendColumnCount(count uint64) error {
	length := misc.LenEncIntSize(count)
	data := c.StartEphemeralPacket(length)
	misc.WriteLenEncInt(data, 0, count)
	return c.WriteEphemeralPacket()
}

func (c *Conn) writeColumnDefinition(field *Field) error {
	length := 4 + // lenEncStringSize("def")
		misc.LenEncStringSize(field.Database) +
		misc.LenEncStringSize(field.Table) +
		misc.LenEncStringSize(field.OrgTable) +
		misc.LenEncStringSize(field.Name) +
		misc.LenEncStringSize(field.OrgName) +
		1 + // length of fixed length fields
		2 + // character set
		4 + // column length
		1 + // type
		2 + // flags
		1 + // decimals
		2 // filler

	// Get the type and the flags back. If the Field contains
	// non-zero flags, we use them. Otherwise use the flags we
	// derive from the type.
	typ, flags := constant.TypeToMySQL(field.FieldType)
	if field.Flags != 0 {
		flags = int64(field.Flags)
	}

	data := c.StartEphemeralPacket(length)
	pos := 0

	pos = misc.WriteLenEncString(data, pos, "def") // Always the same.
	pos = misc.WriteLenEncString(data, pos, field.Database)
	pos = misc.WriteLenEncString(data, pos, field.Table)
	pos = misc.WriteLenEncString(data, pos, field.OrgTable)
	pos = misc.WriteLenEncString(data, pos, field.Name)
	pos = misc.WriteLenEncString(data, pos, field.OrgName)
	pos = misc.WriteByte(data, pos, 0x0c)
	pos = misc.WriteUint16(data, pos, field.CharSet)
	pos = misc.WriteUint32(data, pos, field.ColumnLength)
	pos = misc.WriteByte(data, pos, byte(typ))
	pos = misc.WriteUint16(data, pos, uint16(flags))
	pos = misc.WriteByte(data, pos, byte(field.Decimals))
	pos = misc.WriteUint16(data, pos, uint16(0x0000))

	if pos != len(data) {
		return fmt.Errorf("packing of column definition used %v bytes instead of %v", pos, len(data))
	}

	return c.WriteEphemeralPacket()
}

// WriteFields writes the fields of a Result. It should be called only
// if there are valid Columns in the result.
func (c *Conn) WriteFields(capabilities uint32, fields []*Field) error {
	// Send the number of fields first.
	if err := c.sendColumnCount(uint64(len(fields))); err != nil {
		return err
	}

	// Now send each Field.
	for _, field := range fields {
		fld := field
		if err := c.writeColumnDefinition(fld); err != nil {
			return err
		}
	}

	// Now send an EOF packet.
	if capabilities&constant.CapabilityClientDeprecateEOF == 0 {
		// With CapabilityClientDeprecateEOF, we do not send this EOF.
		if err := c.WriteEOFPacket(c.StatusFlags, 0); err != nil {
			return err
		}
	}
	return nil
}

func (c *Conn) writeRow(row []*proto.Value) error {
	length := 0
	for _, val := range row {
		if val == nil || val.Val == nil {
			length++
		} else {
			l := len(val.Raw)
			length += misc.LenEncIntSize(uint64(l)) + l
		}
	}

	data := c.StartEphemeralPacket(length)
	pos := 0
	for _, val := range row {
		if val == nil || val.Val == nil {
			pos = misc.WriteByte(data, pos, constant.NullValue)
		} else {
			l := len(val.Raw)
			pos = misc.WriteLenEncInt(data, pos, uint64(l))
			pos += copy(data[pos:], val.Raw)
		}
	}

	if pos != length {
		return errors.Errorf("packet row: got %v bytes but expected %v", pos, length)
	}

	return c.WriteEphemeralPacket()
}

// WriteRows sends the rows of a Result.
func (c *Conn) WriteRows(result *Result) error {
	for {
		row, err := result.Rows.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		textRow := TextRow{Row: row}
		values, err := textRow.Decode()
		if err != nil {
			return err
		}
		if err := c.writeRow(values); err != nil {
			return err
		}
	}
	return nil
}

// WritePrepare writes a prepare query response to the wire.
func (c *Conn) WritePrepare(capabilities uint32, prepare *proto.Stmt) error {
	paramsCount := prepare.ParamsCount

	data := c.StartEphemeralPacket(12)
	pos := 0

	pos = misc.WriteByte(data, pos, 0x00)
	pos = misc.WriteUint32(data, pos, prepare.StatementID)
	pos = misc.WriteUint16(data, pos, uint16(0))
	pos = misc.WriteUint16(data, pos, paramsCount)
	pos = misc.WriteByte(data, pos, 0x00)
	misc.WriteUint16(data, pos, 0x0000)

	if err := c.WriteEphemeralPacket(); err != nil {
		return err
	}

	if paramsCount > 0 {
		for i := uint16(0); i < paramsCount; i++ {
			if err := c.writeColumnDefinition(&Field{
				Name:      "?",
				FieldType: constant.FieldTypeString,
				Flags:     constant.BinaryFlag,
				CharSet:   63}); err != nil {
				return err
			}
		}

		// Now send an EOF packet.
		if capabilities&constant.CapabilityClientDeprecateEOF == 0 {
			// With CapabilityClientDeprecateEOF, we do not send this EOF.
			if err := c.WriteEOFPacket(c.StatusFlags, 0); err != nil {
				return err
			}
		}
	}

	return nil
}

// WriteBinaryRow writes text row to binary row
func (c *Conn) writeBinaryRow(fields []*Field, row []*proto.Value) error {
	length := 0
	nullBitMapLen := (len(fields) + 7 + 2) / 8
	for _, val := range row {
		if val != nil && val.Val != nil {
			l, err := packet.Val2MySQLLen(val)
			if err != nil {
				return fmt.Errorf("internal value %v get MySQL value length error: %v", val, err)
			}
			length += l
		}
	}

	length += nullBitMapLen + 1

	data := c.StartEphemeralPacket(length)
	pos := 0

	pos = misc.WriteByte(data, pos, 0x00)

	for i := 0; i < nullBitMapLen; i++ {
		pos = misc.WriteByte(data, pos, 0x00)
	}

	for i, val := range row {
		if val == nil || val.Val == nil {
			bytePos := (i+2)/8 + 1
			bitPos := (i + 2) % 8
			data[bytePos] |= 1 << uint(bitPos)
		} else {
			v, err := packet.Val2MySQL(val)
			if err != nil {
				c.RecycleWritePacket()
				return fmt.Errorf("internal value %v to MySQL value error: %v", val, err)
			}
			pos += copy(data[pos:], v)
		}
	}

	if pos != length {
		return fmt.Errorf("internal error packet row: got %v bytes but expected %v", pos, length)
	}

	return c.WriteEphemeralPacket()
}

// writeTextToBinaryRows sends the rows of a Result with binary form.
func (c *Conn) writeTextToBinaryRows(result *Result) error {
	for {
		row, err := result.Rows.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		textRow := TextRow{Row: row}
		values, err := textRow.Decode()
		if err != nil {
			return err
		}
		if err := c.writeBinaryRow(result.Fields, values); err != nil {
			return err
		}
	}
	return nil
}

func (c *Conn) WriteBinaryRows(result *Result) error {
	for {
		row, err := result.Rows.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if err := c.WritePacket(row.Data()); err != nil {
			return err
		}
	}
	return nil
}

func (c *Conn) WriteRowsDirect(result *MergeResult) error {
	for _, row := range result.Rows {
		if err := c.WritePacket(row.Data()); err != nil {
			return err
		}
	}
	return nil
}

// WriteEndResult concludes the sending of a Result.
// if more is set to true, then it means there are more results afterwords
func (c *Conn) WriteEndResult(capabilities uint32, more bool, affectedRows, lastInsertID uint64, warnings uint16) error {
	// Send either an EOF, or an OK packet.
	// See doc.go.
	flags := c.StatusFlags
	if more {
		flags |= constant.ServerMoreResultsExists
	}
	if capabilities&constant.CapabilityClientDeprecateEOF == 0 {
		if err := c.WriteEOFPacket(flags, warnings); err != nil {
			return err
		}
	} else {
		// This will flush too.
		if err := c.WriteOKPacketWithEOFHeader(affectedRows, lastInsertID, flags, warnings); err != nil {
			return err
		}
	}

	return nil
}

//
// Packet writing methods, for generic packets.
//

// WriteOKPacket writes an OK packet.
// Server -> Client.
// This method returns a generic error, not a SQLError.
func (c *Conn) WriteOKPacket(affectedRows, lastInsertID uint64, flags uint16, warnings uint16) error {
	length := 1 + // OKPacket
		misc.LenEncIntSize(affectedRows) +
		misc.LenEncIntSize(lastInsertID) +
		2 + // flags
		2 // warnings
	data := c.StartEphemeralPacket(length)
	pos := 0
	pos = misc.WriteByte(data, pos, constant.OKPacket)
	pos = misc.WriteLenEncInt(data, pos, affectedRows)
	pos = misc.WriteLenEncInt(data, pos, lastInsertID)
	pos = misc.WriteUint16(data, pos, flags)
	_ = misc.WriteUint16(data, pos, warnings)

	return c.WriteEphemeralPacket()
}

// WriteOKPacketWithEOFHeader writes an OK packet with an EOF header.
// This is used at the end of a result set if
// CapabilityClientDeprecateEOF is set.
// Server -> Client.
// This method returns a generic error, not a SQLError.
func (c *Conn) WriteOKPacketWithEOFHeader(affectedRows, lastInsertID uint64, flags uint16, warnings uint16) error {
	length := 1 + // EOFPacket
		misc.LenEncIntSize(affectedRows) +
		misc.LenEncIntSize(lastInsertID) +
		2 + // flags
		2 // warnings
	data := c.StartEphemeralPacket(length)
	pos := 0
	pos = misc.WriteByte(data, pos, constant.EOFPacket)
	pos = misc.WriteLenEncInt(data, pos, affectedRows)
	pos = misc.WriteLenEncInt(data, pos, lastInsertID)
	pos = misc.WriteUint16(data, pos, flags)
	_ = misc.WriteUint16(data, pos, warnings)

	return c.WriteEphemeralPacket()
}

// WriteErrorPacket writes an error packet.
// Server -> Client.
// This method returns a generic error, not a SQLError.
func (c *Conn) WriteErrorPacket(errorCode uint16, sqlState string, format string, args ...interface{}) error {
	errorMessage := fmt.Sprintf(format, args...)
	length := 1 + 2 + 1 + 5 + len(errorMessage)
	data := c.StartEphemeralPacket(length)
	pos := 0
	pos = misc.WriteByte(data, pos, constant.ErrPacket)
	pos = misc.WriteUint16(data, pos, errorCode)
	pos = misc.WriteByte(data, pos, '#')
	if sqlState == "" {
		sqlState = constant.SSUnknownSQLState
	}
	if len(sqlState) != 5 {
		panic("sqlState has to be 5 characters long")
	}
	pos = misc.WriteEOFString(data, pos, sqlState)
	_ = misc.WriteEOFString(data, pos, errorMessage)

	return c.WriteEphemeralPacket()
}

// WriteErrorPacketFromError writes an error packet, from a regular error.
// See WriteErrorPacket for other info.
func (c *Conn) WriteErrorPacketFromError(err error) error {
	if se, ok := err.(*err2.SQLError); ok {
		return c.WriteErrorPacket(uint16(se.Num), se.State, "%v", se.Message)
	}

	return c.WriteErrorPacket(constant.ERUnknownError, constant.SSUnknownSQLState, "unknown error: %v", err)
}

// WriteEOFPacket writes an EOF packet, through the buffer, and
// doesn't flush (as it is used as part of a query result).
func (c *Conn) WriteEOFPacket(flags uint16, warnings uint16) error {
	length := 5
	data := c.StartEphemeralPacket(length)
	pos := 0
	pos = misc.WriteByte(data, pos, constant.EOFPacket)
	pos = misc.WriteUint16(data, pos, warnings)
	_ = misc.WriteUint16(data, pos, flags)

	return c.WriteEphemeralPacket()
}

// GetTLSClientCerts gets TLS certificates.
func (c *Conn) GetTLSClientCerts() []*x509.Certificate {
	if tlsConn, ok := c.conn.(*tls.Conn); ok {
		return tlsConn.ConnectionState().PeerCertificates
	}
	return nil
}

// RemoteAddr returns the underlying socket RemoteAddr().
func (c *Conn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// ID returns the MySQL connection ID for this connection.
func (c *Conn) ID() int64 {
	return int64(c.ConnectionID)
}

// Ident returns a useful identification string for error logging
func (c *Conn) String() string {
	return fmt.Sprintf("client %v (%s)", c.ConnectionID, c.RemoteAddr().String())
}

// Close closes the connection. It can be called from a different go
// routine to interrupt the current connection.
func (c *Conn) Close() {
	if c.closed.CompareAndSwap(false, true) {
		c.conn.Close()
	}
}

// IsClosed returns true if this connection was ever closed by the
// Close() method.  Note if the other side closes the connection, but
// Close() wasn't called, this will return false.
func (c *Conn) IsClosed() bool {
	return c.closed.Get()
}
