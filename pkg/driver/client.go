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
	"context"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/cectc/dbpack/pkg/constant"
	err2 "github.com/cectc/dbpack/pkg/errors"
	"github.com/cectc/dbpack/pkg/misc"
	"github.com/cectc/dbpack/pkg/mysql"
	"github.com/cectc/dbpack/pkg/packet"
	"github.com/cectc/dbpack/third_party/pools"
)

type Connector struct {
	dataSourceName string
	conf           *Config
}

func NewConnector(dataSourceName, dsn string) (*Connector, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return &Connector{
		dataSourceName: dataSourceName,
		conf:           cfg,
	}, nil
}

func (c *Connector) NewBackendConnection(ctx context.Context) (pools.Resource, error) {
	conn := &BackendConnection{dataSourceName: c.dataSourceName, conf: c.conf}
	err := conn.Connect(ctx)
	return conn, err
}

type BackendConnection struct {
	*mysql.Conn

	dataSourceName string

	conf *Config

	// capabilities is the current set of features this connection
	// is using.  It is the features that are both supported by
	// the client and the server, and currently in use.
	// It is set during the initial handshake.
	//
	// It is only used for CapabilityClientDeprecateEOF
	// and CapabilityClientFoundRows.
	capabilities uint32

	serverVersion string

	characterSet uint8
}

func (conn *BackendConnection) DataSourceName() string {
	return conn.dataSourceName
}

func (conn *BackendConnection) Connect(ctx context.Context) error {
	typ := "tcp"
	if conn.conf.Net == "" {
		if strings.Contains(conn.conf.Addr, "/") {
			typ = "unix"
		}
	} else {
		typ = conn.conf.Net
	}
	netConn, err := net.Dial(typ, conn.conf.Addr)
	if err != nil {
		return err
	}
	tcpConn := netConn.(*net.TCPConn)
	// SetNoDelay controls whether the operating system should delay packet transmission
	// in hopes of sending fewer packets (Nagle's algorithm).
	// The default is true (no delay),
	// meaning that Content is sent as soon as possible after a Write.
	if err := tcpConn.SetNoDelay(true); err != nil {
		return err
	}
	if err := tcpConn.SetKeepAlive(true); err != nil {
		return err
	}

	conn.Conn = mysql.NewConn(tcpConn)

	return conn.clientHandshake()
}

func (conn *BackendConnection) clientHandshake() error {
	// Wait for the server initial handshake packet, and parse it.
	data, err := conn.ReadPacket()
	if err != nil {
		return err2.NewSQLError(constant.CRServerLost, "", "initial packet read failed: %v", err)
	}
	capabilities, salt, plugin, err := conn.parseInitialHandshakePacket(data)
	if err != nil {
		return err
	}

	conn.capabilities = 0
	if !conn.conf.DisableClientDeprecateEOF {
		conn.capabilities = capabilities & (constant.CapabilityClientDeprecateEOF)
	}

	//// Password encryption.
	//scrambledPassword := ScramblePassword(salt, []byte(conn.Passwd))

	authResp, err := conn.auth(salt, plugin)
	if err != nil {
		return err
	}

	// Build and send our handshake response 41.
	// Note this one will never have SSL flag on.
	if err := conn.writeHandshakeResponse41(capabilities, authResp, plugin); err != nil {
		return err
	}

	// Handle response to auth packet, switch methods if possible
	if err = conn.handleAuthResult(salt, plugin); err != nil {
		// Authentication failed and MySQL has already closed the connection
		// (https://dev.mysql.com/doc/internals/en/authentication-fails.html).
		// Do not send COM_QUIT, just cleanup and return the error.
		conn.Close()
		return err
	}

	// If the server didn't support DbName in its handshake, set
	// it now. This is what the 'mysql' client does.
	if capabilities&constant.CapabilityClientConnectWithDB == 0 && conn.conf.DBName != "" {
		// Write the packet.
		if err := conn.WriteComInitDB(conn.conf.DBName); err != nil {
			return err
		}

		// Wait for response, should be OK.
		response, err := conn.ReadPacket()
		conn.RecycleReadPacket()
		if err != nil {
			return err2.NewSQLError(constant.CRServerLost, constant.SSUnknownSQLState, "%v", err)
		}
		switch response[0] {
		case constant.OKPacket:
			// OK packet, we are authenticated.
			return nil
		case constant.ErrPacket:
			return packet.ParseErrorPacket(response)
		default:
			// FIXME(alainjobart) handle extra auth cases and so on.
			return err2.NewSQLError(constant.CRServerHandshakeErr, constant.SSUnknownSQLState, "initial server response is asking for more information, not implemented yet: %v", response)
		}
	}

	return nil
}

// parseInitialHandshakePacket parses the initial handshake from the server.
// It returns a SQLError with the right code.
func (conn *BackendConnection) parseInitialHandshakePacket(data []byte) (uint32, []byte, string, error) {
	pos := 0

	// Protocol version.
	pver, pos, ok := misc.ReadByte(data, pos)
	if !ok {
		return 0, nil, "", err2.NewSQLError(constant.CRVersionError, constant.SSUnknownSQLState, "parseInitialHandshakePacket: packet has no protocol version")
	}

	// Server is allowed to immediately send ERR packet
	if pver == constant.ErrPacket {
		errorCode, pos, _ := misc.ReadUint16(data, pos)
		// Normally there would be a 1-byte sql_state_marker field and a 5-byte
		// sql_state field here, but docs say these will not be present in this case.
		errorMsg, _, _ := misc.ReadEOFString(data, pos)
		return 0, nil, "", err2.NewSQLError(constant.CRServerHandshakeErr, constant.SSUnknownSQLState, "immediate error from server errorCode=%v errorMsg=%v", errorCode, errorMsg)
	}

	if pver != constant.ProtocolVersion {
		return 0, nil, "", err2.NewSQLError(constant.CRVersionError, constant.SSUnknownSQLState, "bad protocol version: %v", pver)
	}

	// Read the server version.
	conn.serverVersion, pos, ok = misc.ReadNullString(data, pos)
	if !ok {
		return 0, nil, "", err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "parseInitialHandshakePacket: packet has no server version")
	}

	// Read the connection id.
	conn.ConnectionID, pos, ok = misc.ReadUint32(data, pos)
	if !ok {
		return 0, nil, "", err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "parseInitialHandshakePacket: packet has no connection id")
	}

	// Read the first part of the auth-plugin-Content
	authPluginData, pos, ok := misc.ReadBytes(data, pos, 8)
	if !ok {
		return 0, nil, "", err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "parseInitialHandshakePacket: packet has no auth-plugin-Content-part-1")
	}

	// One byte filler, 0. We don't really care about the value.
	_, pos, ok = misc.ReadByte(data, pos)
	if !ok {
		return 0, nil, "", err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "parseInitialHandshakePacket: packet has no filler")
	}

	// Lower 2 bytes of the capability flags.
	capLower, pos, ok := misc.ReadUint16(data, pos)
	if !ok {
		return 0, nil, "", err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "parseInitialHandshakePacket: packet has no capability flags (lower 2 bytes)")
	}
	var capabilities = uint32(capLower)

	// The packet can end here.
	if pos == len(data) {
		return capabilities, authPluginData, "", nil
	}

	// Character set.
	characterSet, pos, ok := misc.ReadByte(data, pos)
	if !ok {
		return 0, nil, "", err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "parseInitialHandshakePacket: packet has no character set")
	}
	conn.characterSet = characterSet

	// Status flags. Ignored.
	_, pos, ok = misc.ReadUint16(data, pos)
	if !ok {
		return 0, nil, "", err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "parseInitialHandshakePacket: packet has no status flags")
	}

	// Upper 2 bytes of the capability flags.
	capUpper, pos, ok := misc.ReadUint16(data, pos)
	if !ok {
		return 0, nil, "", err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "parseInitialHandshakePacket: packet has no capability flags (upper 2 bytes)")
	}
	capabilities += uint32(capUpper) << 16

	// Length of auth-plugin-Content, or 0.
	// Only with CLIENT_PLUGIN_AUTH capability.
	var authPluginDataLength byte
	if capabilities&constant.CapabilityClientPluginAuth != 0 {
		authPluginDataLength, pos, ok = misc.ReadByte(data, pos)
		if !ok {
			return 0, nil, "", err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "parseInitialHandshakePacket: packet has no length of auth-plugin-Content")
		}
	} else {
		// One byte filler, 0. We don't really care about the value.
		_, pos, ok = misc.ReadByte(data, pos)
		if !ok {
			return 0, nil, "", err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "parseInitialHandshakePacket: packet has no length of auth-plugin-Content filler")
		}
	}

	// 10 reserved 0 bytes.
	pos += 10

	if capabilities&constant.CapabilityClientSecureConnection != 0 {
		// The next part of the auth-plugin-Content.
		// The length is max(13, length of auth-plugin-Content - 8).
		l := int(authPluginDataLength) - 8
		if l > 13 {
			l = 13
		}
		var authPluginDataPart2 []byte
		authPluginDataPart2, pos, ok = misc.ReadBytes(data, pos, l)
		if !ok {
			return 0, nil, "", err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "parseInitialHandshakePacket: packet has no auth-plugin-Content-part-2")
		}

		// The last byte has to be 0, and is not part of the Content.
		if authPluginDataPart2[l-1] != 0 {
			return 0, nil, "", err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "parseInitialHandshakePacket: auth-plugin-Content-part-2 is not 0 terminated")
		}
		authPluginData = append(authPluginData, authPluginDataPart2[0:l-1]...)
	}

	// Auth-plugin name.
	if capabilities&constant.CapabilityClientPluginAuth != 0 {
		authPluginName, _, ok := misc.ReadNullString(data, pos)
		if !ok {
			// Fallback for versions prior to 5.5.10 and
			// 5.6.2 that don't have a null terminated string.
			authPluginName = string(data[pos : len(data)-1])
		}

		return capabilities, authPluginData, authPluginName, nil
	}

	return capabilities, authPluginData, constant.MysqlNativePassword, nil
}

// writeHandshakeResponse41 writes the handshake response.
// Returns a SQLError.
func (conn *BackendConnection) writeHandshakeResponse41(capabilities uint32, scrambledPassword []byte, plugin string) error {
	// Build our flags.
	var flags uint32 = constant.CapabilityClientLongPassword |
		constant.CapabilityClientLongFlag |
		constant.CapabilityClientProtocol41 |
		constant.CapabilityClientTransactions |
		constant.CapabilityClientSecureConnection |
		constant.CapabilityClientMultiStatements |
		constant.CapabilityClientMultiResults |
		constant.CapabilityClientPluginAuth |
		constant.CapabilityClientPluginAuthLenencClientData |
		// If the server supported
		// CapabilityClientDeprecateEOF, we also support it.
		conn.capabilities&constant.CapabilityClientDeprecateEOF

	if conn.conf.ClientFoundRows {
		// Pass-through ClientFoundRows flag.
		flags |= constant.CapabilityClientFoundRows
	}

	// FIXME(alainjobart) add multi statement.

	length :=
		4 + // Client capability flags.
			4 + // Max-packet size.
			1 + // Character set.
			23 + // Reserved.
			misc.LenNullString(conn.conf.User) +
			// length of scrambled password is handled below.
			len(scrambledPassword) +
			21 + // "mysql_native_password" string.
			1 // terminating zero.

	// Add the DB name if the server supports it.
	if conn.conf.DBName != "" && (capabilities&constant.CapabilityClientConnectWithDB != 0) {
		flags |= constant.CapabilityClientConnectWithDB
		length += misc.LenNullString(conn.conf.DBName)
	}

	if capabilities&constant.CapabilityClientPluginAuthLenencClientData != 0 {
		length += misc.LenEncIntSize(uint64(len(scrambledPassword)))
	} else {
		length++
	}

	data := conn.StartEphemeralPacket(length)
	pos := 0

	// Client capability flags.
	pos = misc.WriteUint32(data, pos, flags)

	// Max-packet size, always 0. See doc.go.
	pos = misc.WriteZeroes(data, pos, 4)

	// Character set.
	pos = misc.WriteByte(data, pos, byte(constant.Collations[conn.conf.Collation]))

	// 23 reserved bytes, all 0.
	pos = misc.WriteZeroes(data, pos, 23)

	// Username
	pos = misc.WriteNullString(data, pos, conn.conf.User)

	// Scrambled password.  The length is encoded as variable length if
	// CapabilityClientPluginAuthLenencClientData is set.
	if capabilities&constant.CapabilityClientPluginAuthLenencClientData != 0 {
		pos = misc.WriteLenEncInt(data, pos, uint64(len(scrambledPassword)))
	} else {
		data[pos] = byte(len(scrambledPassword))
		pos++
	}
	pos += copy(data[pos:], scrambledPassword)

	// DbName, only if server supports it.
	if conn.conf.DBName != "" && (capabilities&constant.CapabilityClientConnectWithDB != 0) {
		pos = misc.WriteNullString(data, pos, conn.conf.DBName)
	}

	// Assume native client during response
	pos = misc.WriteNullString(data, pos, plugin)

	// Sanity-check the length.
	if pos != len(data) {
		return err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "writeHandshakeResponse41: only packed %v bytes, out of %v allocated", pos, len(data))
	}

	if err := conn.WriteEphemeralPacket(); err != nil {
		return err2.NewSQLError(constant.CRServerLost, constant.SSUnknownSQLState, "cannot send HandshakeResponse41: %v", err)
	}
	return nil
}

// WriteComInitDB changes the default database to use.
// Client -> Server.
// Returns SQLError(CRServerGone) if it can't.
func (conn *BackendConnection) WriteComInitDB(db string) error {
	// This is a new command, need to reset the sequence.
	conn.ResetSequence()

	data := conn.StartEphemeralPacket(len(db) + 1)
	data[0] = constant.ComInitDB
	copy(data[1:], db)
	if err := conn.WriteEphemeralPacket(); err != nil {
		return err2.NewSQLError(constant.CRServerGone, constant.SSUnknownSQLState, err.Error())
	}
	return nil
}

// WriteComQuit writes a Quit message for the server, to indicate we
// want to close the connection.
// Client -> Server.
// Returns SQLError(CRServerGone) if it can't.
func (conn *BackendConnection) WriteComQuit() error {
	// This is a new command, need to reset the Sequence.
	conn.ResetSequence()

	data := conn.StartEphemeralPacket(1)
	data[0] = constant.ComQuit
	if err := conn.WriteEphemeralPacket(); err != nil {
		return err2.NewSQLError(constant.CRServerGone, constant.SSUnknownSQLState, err.Error())
	}
	return nil
}

// WriteComQuery writes a query for the server to execute.
// Client -> Server.
// Returns SQLError(CRServerGone) if it can't.
func (conn *BackendConnection) WriteComQuery(query string) error {
	// This is a new command, need to reset the sequence.
	conn.ResetSequence()

	data := conn.StartEphemeralPacket(len(query) + 1)
	data[0] = constant.ComQuery
	copy(data[1:], query)
	if err := conn.WriteEphemeralPacket(); err != nil {
		return err2.NewSQLError(constant.CRServerGone, constant.SSUnknownSQLState, err.Error())
	}
	return nil
}

// WriteComSetOption changes the connection's capability of executing multi statements.
// Returns SQLError(CRServerGone) if it can't.
func (conn *BackendConnection) WriteComSetOption(operation uint16) error {
	// This is a new command, need to reset the sequence.
	conn.ResetSequence()

	data := conn.StartEphemeralPacket(16 + 1)
	data[0] = constant.ComSetOption
	misc.WriteUint16(data, 1, operation)
	if err := conn.WriteEphemeralPacket(); err != nil {
		return err2.NewSQLError(constant.CRServerGone, constant.SSUnknownSQLState, err.Error())
	}
	return nil
}

func (conn *BackendConnection) WriteComFieldList(table string, wildcard string) error {
	conn.ResetSequence()
	length := 1 +
		misc.LenNullString(table) +
		misc.LenNullString(wildcard)

	data := conn.StartEphemeralPacket(length)
	pos := 0

	pos = misc.WriteByte(data, 0, constant.ComFieldList)
	pos = misc.WriteNullString(data, pos, table)
	misc.WriteNullString(data, pos, wildcard)

	if err := conn.WriteEphemeralPacket(); err != nil {
		return err
	}

	return nil
}

// WriteComStmtClose close statement
func (conn *BackendConnection) WriteComStmtClose(statementID uint32) (err error) {
	// This is a new command, need to reset the Sequence.
	conn.ResetSequence()

	data := conn.StartEphemeralPacket(4 + 1)
	data[0] = constant.ComStmtClose
	misc.WriteUint32(data, 1, statementID)
	if err := conn.WriteEphemeralPacket(); err != nil {
		return err2.NewSQLError(constant.CRServerGone, constant.SSUnknownSQLState, err.Error())
	}

	return conn.readResultOK()
}

// ReadQueryResult gets the result from the last written query.
func (conn *BackendConnection) ReadQueryResult(wantFields bool) (result *mysql.Result, more bool, warnings uint16, err error) {
	// Get the result.
	affectedRows, lastInsertID, colNumber, more, warnings, err := conn.ReadComQueryResponse()
	if err != nil {
		return nil, false, 0, err
	}

	if colNumber == 0 {
		// OK packet, means no results. Just use the numbers.
		return &mysql.Result{
			AffectedRows: affectedRows,
			InsertId:     lastInsertID,
		}, more, warnings, nil
	}

	result = &mysql.Result{
		Fields: make([]*mysql.Field, colNumber),
	}

	// Read column headers. One packet per column.
	// Build the fields.
	for i := 0; i < colNumber; i++ {
		field := &mysql.Field{}
		result.Fields[i] = field

		if wantFields {
			if err := conn.ReadColumnDefinition(field, i); err != nil {
				return nil, false, 0, err
			}
		} else {
			if err := conn.ReadColumnDefinitionType(field, i); err != nil {
				return nil, false, 0, err
			}
		}
	}

	if conn.capabilities&constant.CapabilityClientDeprecateEOF == 0 {
		// EOF is only present here if it's not deprecated.
		data, err := conn.ReadEphemeralPacket()
		if err != nil {
			return nil, false, 0, err2.NewSQLError(constant.CRServerLost, constant.SSUnknownSQLState, "%v", err)
		}
		if packet.IsEOFPacket(data) {

			// This is what we expect.
			// Warnings and status flags are ignored.
			conn.RecycleReadPacket()
			// goto: read row loop

		} else if packet.IsErrorPacket(data) {
			defer conn.RecycleReadPacket()
			return nil, false, 0, packet.ParseErrorPacket(data)
		} else {
			defer conn.RecycleReadPacket()
			return nil, false, 0, fmt.Errorf("unexpected packet after fields: %v", data)
		}
	}

	result.Rows = mysql.NewRows(conn.Conn, result.Fields)
	return
}

func (conn *BackendConnection) ReadComQueryResponse() (affectedRows uint64, lastInsertID uint64, status int, more bool, warnings uint16, err error) {
	data, err := conn.ReadEphemeralPacket()
	if err != nil {
		return 0, 0, 0, false, 0, err2.NewSQLError(constant.CRServerLost, constant.SSUnknownSQLState, "%v", err)
	}
	defer conn.RecycleReadPacket()
	if len(data) == 0 {
		return 0, 0, 0, false, 0, err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "invalid empty COM_QUERY response packet")
	}

	switch data[0] {
	case constant.OKPacket:
		affectedRows, lastInsertID, status, warnings, err := packet.ParseOKPacket(data)
		return affectedRows, lastInsertID, 0, (status & constant.ServerMoreResultsExists) != 0, warnings, err
	case constant.ErrPacket:
		// Error
		return 0, 0, 0, false, 0, packet.ParseErrorPacket(data)
	case 0xfb:
		// Local infile
		return 0, 0, 0, false, 0, fmt.Errorf("not implemented")
	}
	n, pos, ok := misc.ReadLenEncInt(data, 0)
	if !ok {
		return 0, 0, 0, false, 0, err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "cannot get column number")
	}
	if pos != len(data) {
		return 0, 0, 0, false, 0, err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "extra Content in COM_QUERY response")
	}
	return 0, 0, int(n), false, 0, nil
}

// ReadColumnDefinition reads the next Column Definition packet.
// Returns a SQLError.
func (conn *BackendConnection) ReadColumnDefinition(field *mysql.Field, index int) error {
	colDef, err := conn.ReadEphemeralPacket()
	if err != nil {
		return err2.NewSQLError(constant.CRServerLost, constant.SSUnknownSQLState, "%v", err)
	}
	defer conn.RecycleReadPacket()

	if packet.IsEOFPacket(colDef) {
		return io.EOF
	}

	// Catalog is ignored, always set to "def"
	pos, ok := misc.SkipLenEncString(colDef, 0)
	if !ok {
		return err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "skipping col %v catalog failed", index)
	}

	// schema, table, orgTable, name and OrgName are strings.
	field.Database, pos, ok = misc.ReadLenEncString(colDef, pos)
	if !ok {
		return err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "extracting col %v schema failed", index)
	}
	field.Table, pos, ok = misc.ReadLenEncString(colDef, pos)
	if !ok {
		return err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "extracting col %v table failed", index)
	}
	field.OrgTable, pos, ok = misc.ReadLenEncString(colDef, pos)
	if !ok {
		return err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "extracting col %v org_table failed", index)
	}
	field.Name, pos, ok = misc.ReadLenEncString(colDef, pos)
	if !ok {
		return err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "extracting col %v name failed", index)
	}
	field.OrgName, pos, ok = misc.ReadLenEncString(colDef, pos)
	if !ok {
		return err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "extracting col %v org_name failed", index)
	}

	// Skip length of fixed-length fields.
	pos++

	// characterSet is a uint16.
	characterSet, pos, ok := misc.ReadUint16(colDef, pos)
	if !ok {
		return err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "extracting col %v characterSet failed", index)
	}
	field.CharSet = characterSet

	// columnLength is a uint32.
	field.ColumnLength, pos, ok = misc.ReadUint32(colDef, pos)
	if !ok {
		return err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "extracting col %v columnLength failed", index)
	}

	// type is one byte.
	t, pos, ok := misc.ReadByte(colDef, pos)
	if !ok {
		return err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "extracting col %v type failed", index)
	}

	// flags is 2 bytes.
	flags, pos, ok := misc.ReadUint16(colDef, pos)
	if !ok {
		return err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "extracting col %v flags failed", index)
	}
	field.Flags = uint(flags)

	// Convert MySQL type to Vitess type.
	field.FieldType, err = constant.MySQLToType(int64(t), int64(flags))
	if err != nil {
		return err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "MySQLToType(%v,%v) failed for column %v: %v", t, flags, index, err)
	}
	// Decimals is a byte.
	decimals, pos, ok := misc.ReadByte(colDef, pos)
	if !ok {
		return err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "extracting col %v decimals failed", index)
	}
	field.Decimals = decimals

	//if more Content, command was field list
	if len(colDef) > pos+8 {
		//length of default value lenenc-int
		field.DefaultValueLength, pos, ok = misc.ReadUint64(colDef, pos)
		if !ok {
			return err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "extracting col %v default value failed", index)
		}

		if pos+int(field.DefaultValueLength) > len(colDef) {
			return err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "extracting col %v default value failed", index)
		}

		//default value string[$len]
		field.DefaultValue = colDef[pos:(pos + int(field.DefaultValueLength))]
	}
	return nil
}

// ReadColumnDefinitionType is a faster version of
// ReadColumnDefinition that only fills in the Type.
// Returns a SQLError.
func (conn *BackendConnection) ReadColumnDefinitionType(field *mysql.Field, index int) error {
	colDef, err := conn.ReadEphemeralPacket()
	if err != nil {
		return err2.NewSQLError(constant.CRServerLost, constant.SSUnknownSQLState, "%v", err)
	}
	defer conn.RecycleReadPacket()

	// catalog, schema, table, orgTable, name and orgName are
	// strings, all skipped.
	pos, ok := misc.SkipLenEncString(colDef, 0)
	if !ok {
		return err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "skipping col %v catalog failed", index)
	}
	pos, ok = misc.SkipLenEncString(colDef, pos)
	if !ok {
		return err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "skipping col %v schema failed", index)
	}
	pos, ok = misc.SkipLenEncString(colDef, pos)
	if !ok {
		return err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "skipping col %v table failed", index)
	}
	pos, ok = misc.SkipLenEncString(colDef, pos)
	if !ok {
		return err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "skipping col %v org_table failed", index)
	}
	pos, ok = misc.SkipLenEncString(colDef, pos)
	if !ok {
		return err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "skipping col %v name failed", index)
	}
	pos, ok = misc.SkipLenEncString(colDef, pos)
	if !ok {
		return err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "skipping col %v org_name failed", index)
	}

	// Skip length of fixed-length fields.
	pos++

	// characterSet is a uint16.
	_, pos, ok = misc.ReadUint16(colDef, pos)
	if !ok {
		return err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "extracting col %v characterSet failed", index)
	}

	// columnLength is a uint32.
	_, pos, ok = misc.ReadUint32(colDef, pos)
	if !ok {
		return err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "extracting col %v columnLength failed", index)
	}

	// type is one byte
	t, pos, ok := misc.ReadByte(colDef, pos)
	if !ok {
		return err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "extracting col %v type failed", index)
	}

	// flags is 2 bytes
	flags, _, ok := misc.ReadUint16(colDef, pos)
	if !ok {
		return err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "extracting col %v flags failed", index)
	}

	// Convert MySQL type to Vitess type.
	field.FieldType, err = constant.MySQLToType(int64(t), int64(flags))
	if err != nil {
		return err2.NewSQLError(constant.CRMalformedPacket, constant.SSUnknownSQLState, "MySQLToType(%v,%v) failed for column %v: %v", t, flags, index, err)
	}

	// skip decimals

	return nil
}

// DrainResults will read all packets for a result set and ignore them.
func (conn *BackendConnection) DrainResults() error {
	for {
		data, err := conn.ReadEphemeralPacket()
		if err != nil {
			return err2.NewSQLError(constant.CRServerLost, constant.SSUnknownSQLState, "%v", err)
		}
		if packet.IsEOFPacket(data) {
			conn.RecycleReadPacket()
			return nil
		} else if packet.IsErrorPacket(data) {
			defer conn.RecycleReadPacket()
			return packet.ParseErrorPacket(data)
		}
		conn.RecycleReadPacket()
	}
}

func (conn *BackendConnection) ReadColumnDefinitions() ([]*mysql.Field, error) {
	result := make([]*mysql.Field, 0)
	i := 0
	for {
		field := &mysql.Field{}
		err := conn.ReadColumnDefinition(field, i)
		if err == io.EOF {
			return result, nil
		}
		if err != nil {
			return nil, err
		}
		result = append(result, field)
		i++
	}
}

// Ping implements driver.Pinger interface
func (conn *BackendConnection) Ping(ctx context.Context) (err error) {
	// This is a new command, need to reset the Sequence.
	conn.ResetSequence()

	data := conn.StartEphemeralPacket(1)
	data[0] = constant.ComPing
	if err := conn.WriteEphemeralPacket(); err != nil {
		return err2.NewSQLError(constant.CRServerGone, constant.SSUnknownSQLState, err.Error())
	}

	return conn.readResultOK()
}

// Execute executes a query and returns the result.
// Returns a SQLError. Depending on the transport used, the error
// returned might be different for the same condition:
//
// 1. if the server closes the connection when no command is in flight:
//
//   1.1 unix: WriteComQuery will fail with a 'broken pipe', and we'll
//       return CRServerGone(2006).
//
//   1.2 tcp: WriteComQuery will most likely work, but ReadComQueryResponse
//       will fail, and we'll return CRServerLost(2013).
//
//       This is because closing a TCP socket on the server side sends
//       a FIN to the client (telling the client the server is done
//       writing), but on most platforms doesn't send a RST.  So the
//       client has no idea it can't write. So it succeeds writing Content, which
//       *then* triggers the server to send a RST back, received a bit
//       later. By then, the client has already started waiting for
//       the response, and will just return a CRServerLost(2013).
//       So CRServerGone(2006) will almost never be seen with TCP.
//
// 2. if the server closes the connection when a command is in flight,
//    ReadComQueryResponse will fail, and we'll return CRServerLost(2013).
func (conn *BackendConnection) Execute(query string, wantFields bool) (result *mysql.Result, err error) {
	result, _, err = conn.ExecuteMulti(query, wantFields)
	return
}

// ExecuteMulti is for fetching multiple results from a multi-statement result.
// It returns an additional 'more' flag. If it is set, you must fetch the additional
// results using ReadQueryResult.
func (conn *BackendConnection) ExecuteMulti(query string, wantFields bool) (result *mysql.Result, more bool, err error) {
	defer func() {
		if err != nil {
			if sqlerr, ok := err.(*err2.SQLError); ok {
				sqlerr.Query = query
			}
		}
	}()

	// Send the query as a COM_QUERY packet.
	if err = conn.WriteComQuery(query); err != nil {
		return nil, false, err
	}

	result, more, _, err = conn.ReadQueryResult(wantFields)
	return
}

// ExecuteWithWarningCount is for fetching results and a warning count
// Note: In a future iteration this should be abolished and merged into the
// Execute API.
func (conn *BackendConnection) ExecuteWithWarningCount(query string, wantFields bool) (result *mysql.Result, warnings uint16, err error) {
	defer func() {
		if err != nil {
			if sqlerr, ok := err.(*err2.SQLError); ok {
				sqlerr.Query = query
			}
		}
	}()

	// Send the query as a COM_QUERY packet.
	if err = conn.WriteComQuery(query); err != nil {
		return nil, 0, err
	}

	result, _, warnings, err = conn.ReadQueryResult(wantFields)
	return
}

func (conn *BackendConnection) PrepareExecuteArgs(query string, args []interface{}) (result *mysql.Result, warnings uint16, err error) {
	stmt, err := conn.prepare(query)
	if err != nil {
		return nil, 0, err
	}
	return stmt.execArgs(args)
}

func (conn *BackendConnection) PrepareQueryArgs(query string, data []interface{}) (Result *mysql.Result, warnings uint16, err error) {
	stmt, err := conn.prepare(query)
	if err != nil {
		return nil, 0, err
	}
	return stmt.queryArgs(data)
}

func (conn *BackendConnection) PrepareExecute(query string, data []byte) (result *mysql.Result, warnings uint16, err error) {
	stmt, err := conn.prepare(query)
	if err != nil {
		return nil, 0, err
	}
	return stmt.exec(data)
}

func (conn *BackendConnection) PrepareQuery(query string, data []byte) (Result *mysql.Result, warnings uint16, err error) {
	stmt, err := conn.prepare(query)
	if err != nil {
		return nil, 0, err
	}
	return stmt.query(data)
}

func (conn *BackendConnection) prepare(query string) (*BackendStatement, error) {
	// This is a new command, need to reset the sequence.
	conn.ResetSequence()

	data := conn.StartEphemeralPacket(len(query) + 1)
	data[0] = constant.ComPrepare
	copy(data[1:], query)
	if err := conn.WriteEphemeralPacket(); err != nil {
		return nil, err2.NewSQLError(constant.CRServerGone, constant.SSUnknownSQLState, err.Error())
	}

	stmt := &BackendStatement{
		conn: conn,
		sql:  query,
	}

	// Read Result
	columnCount, err := stmt.readPrepareResultPacket()
	if err == nil {
		if stmt.paramCount > 0 {
			if err = conn.DrainResults(); err != nil {
				return nil, err
			}
		}

		if columnCount > 0 {
			err = conn.DrainResults()
		}
	}

	return stmt, err
}
