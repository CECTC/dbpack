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

package listener

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/uber-go/atomic"

	"github.com/cectc/dbpack/pkg/config"
	"github.com/cectc/dbpack/pkg/constant"
	err2 "github.com/cectc/dbpack/pkg/errors"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/misc"
	"github.com/cectc/dbpack/pkg/mysql"
	"github.com/cectc/dbpack/pkg/packet"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/pkg/tracing"
	"github.com/cectc/dbpack/pkg/visitor"
	"github.com/cectc/dbpack/third_party/parser"
)

const initClientConnStatus = constant.ServerStatusAutocommit

type MysqlConfig struct {
	Users         map[string]string `yaml:"users" json:"users"`
	ServerVersion string            `yaml:"server_version" json:"server_version"`
}

type MysqlListener struct {
	// conf
	conf MysqlConfig

	// This is the main listener socket.
	listener net.Listener

	executor proto.Executor

	// Incrementing ID for connection id.
	connectionID uint32
	// connReadBufferSize is size of buffer for reads from underlying connection.
	// Reads are unbuffered if it's <=0.
	connReadBufferSize int

	// capabilities is the current set of features this connection
	// is using.  It is the features that are both supported by
	// the client and the server, and currently in use.
	// It is set during the initial handshake.
	//
	// It is only used for CapabilityClientDeprecateEOF
	// and CapabilityClientFoundRows.
	capabilities uint32

	// characterSet is the character set used by the other side of the
	// connection.
	// It is set during the initial handshake.
	// See the values in constants.go.
	characterSet uint8

	// schemaName is the default database name to use. It is set
	// during handshake, and by ComInitDb packets. Both client and
	// servers maintain it. This member is private because it's
	// non-authoritative: the client can change the schema name
	// through the 'USE' statement, which will bypass this variable.
	schemaName string

	// statementID is the prepared statement ID.
	statementID *atomic.Uint32

	// stmts is the map to use a prepared statement.
	stmts *sync.Map
}

func NewMysqlListener(conf *config.Listener) (proto.Listener, error) {
	var (
		err     error
		content []byte
		cfg     MysqlConfig
	)

	if content, err = json.Marshal(conf.Config); err != nil {
		return nil, errors.Wrap(err, "marshal mysql listener config failed.")
	}
	if err = json.Unmarshal(content, &cfg); err != nil {
		log.Errorf("unmarshal mysql listener config failed, %s", err)
		return nil, err
	}

	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", conf.SocketAddress.Address, conf.SocketAddress.Port))
	if err != nil {
		log.Errorf("listen %s:%d error, %s", conf.SocketAddress.Address, conf.SocketAddress.Port, err)
		return nil, err
	}

	listener := &MysqlListener{
		conf:        cfg,
		listener:    l,
		statementID: atomic.NewUint32(0),
		stmts:       &sync.Map{},
	}
	return listener, nil
}

func (l *MysqlListener) SetExecutor(executor proto.Executor) {
	l.executor = executor
}

func (l *MysqlListener) Listen() {
	log.Infof("start mysql listener %s", l.listener.Addr())
	for {
		conn, err := l.listener.Accept()
		if err != nil {
			return
		}

		l.connectionID++
		connectionID := l.connectionID
		go l.handle(conn, connectionID)
	}
}

func (l *MysqlListener) Close() {
	if err := l.listener.Close(); err != nil {
		log.Error(err)
	}
}

func (l *MysqlListener) handle(conn net.Conn, connectionID uint32) {
	c := mysql.NewConn(conn)
	c.SetConnectionID(connectionID)

	// Catch panics, and close the connection in any case.
	defer func() {
		if x := recover(); x != nil {
			log.Errorf("mysql_server caught panic:\n%v", x)
		}

		if err := conn.Close(); err != nil {
			log.Errorf("connection close error, connection id: %v, error: %s", l.connectionID, err)
		}
		l.executor.ConnectionClose(proto.WithConnectionID(context.Background(), l.connectionID))
	}()

	err := l.handshake(c)
	if err != nil {
		writeErr := c.WriteErrorPacketFromError(err)
		if writeErr != nil {
			log.Errorf("Cannot write error packet to %s: %v", c, writeErr)
			return
		}
		return
	}

	// Negotiation worked, send OK packet.
	if err := c.WriteOKPacket(0, 0, c.StatusFlags(), 0); err != nil {
		log.Errorf("Cannot write OK packet to %s: %v", c, err)
		return
	}
	log.Debugf("connection established, id: %d", connectionID)

	for {
		c.ResetSequence()
		var data []byte
		data, err = c.ReadEphemeralPacket()
		if err != nil {
			c.RecycleReadPacket()
			return
		}

		content := make([]byte, len(data))
		copy(content, data)
		ctx := proto.WithVariableMap(context.Background())
		ctx = proto.WithConnectionID(ctx, connectionID)
		ctx = proto.WithUserName(ctx, c.UserName())
		ctx = proto.WithRemoteAddr(ctx, c.RemoteAddr().String())
		ctx = proto.WithSchema(ctx, l.schemaName)
		err = l.ExecuteCommand(ctx, c, content)
		if err != nil {
			return
		}
	}
}

func (l *MysqlListener) handshake(c *mysql.Conn) error {
	salt, err := newSalt()
	if err != nil {
		return err
	}
	// First build and send the server handshake packet.
	err = l.writeHandshakeV10(c, false, salt)
	if err != nil {
		if err != io.EOF {
			log.Errorf("Cannot send HandshakeV10 packet to %s: %v", c, err)
		}
		return err
	}

	// Wait for the client response. This has to be a direct read,
	// so we don't buffer the TLS negotiation packets.
	response, err := c.ReadEphemeralPacketDirect()
	if err != nil {
		// Don't log EOF errors. They cause too much spam, same as main read loop.
		if err != io.EOF {
			log.Infof("Cannot read client handshake response from %s: %v, it may not be a valid MySQL client", c, err)
		}
		return err
	}

	c.RecycleReadPacket()

	user, _, authResponse, err := l.parseClientHandshakePacket(true, response)
	if err != nil {
		log.Errorf("Cannot parse client handshake response from %s: %v", c, err)
		return err
	}

	err = l.ValidateHash(user, salt, authResponse)
	if err != nil {
		log.Errorf("Error authenticating user using MySQL native password: %v", err)
		return err
	}
	c.SetUserName(user)
	return nil
}

// writeHandshakeV10 writes the Initial Handshake Packet, server side.
// It returns the salt Content.
func (l *MysqlListener) writeHandshakeV10(c *mysql.Conn, enableTLS bool, salt []byte) error {
	capabilities := constant.CapabilityClientLongPassword |
		constant.CapabilityClientFoundRows |
		constant.CapabilityClientLongFlag |
		constant.CapabilityClientConnectWithDB |
		constant.CapabilityClientProtocol41 |
		constant.CapabilityClientTransactions |
		constant.CapabilityClientSecureConnection |
		constant.CapabilityClientMultiStatements |
		constant.CapabilityClientMultiResults |
		constant.CapabilityClientPluginAuth |
		constant.CapabilityClientPluginAuthLenencClientData |
		constant.CapabilityClientDeprecateEOF |
		constant.CapabilityClientConnAttr
	if enableTLS {
		capabilities |= constant.CapabilityClientSSL
	}

	length :=
		1 + // protocol version
			misc.LenNullString(l.conf.ServerVersion) +
			4 + // connection ID
			8 + // first part of salt Content
			1 + // filler byte
			2 + // capability flags (lower 2 bytes)
			1 + // character set
			2 + // status flag
			2 + // capability flags (upper 2 bytes)
			1 + // length of auth plugin Content
			10 + // reserved (0)
			13 + // auth-plugin-Content
			misc.LenNullString(constant.MysqlNativePassword) // auth-plugin-name

	data := c.StartEphemeralPacket(length)
	pos := 0

	// Protocol version.
	pos = misc.WriteByte(data, pos, constant.ProtocolVersion)

	// Copy server version.
	pos = misc.WriteNullString(data, pos, l.conf.ServerVersion)

	// Add connectionID in.
	pos = misc.WriteUint32(data, pos, c.ID())

	pos += copy(data[pos:], salt[:8])

	// One filler byte, always 0.
	pos = misc.WriteByte(data, pos, 0)

	// Lower part of the capability flags.
	pos = misc.WriteUint16(data, pos, uint16(capabilities))

	// Character set.
	pos = misc.WriteByte(data, pos, constant.CharacterSetUtf8)

	// Status flag.
	pos = misc.WriteUint16(data, pos, initClientConnStatus)

	// Upper part of the capability flags.
	pos = misc.WriteUint16(data, pos, uint16(capabilities>>16))

	// Length of auth plugin Content.
	// Always 21 (8 + 13).
	pos = misc.WriteByte(data, pos, 21)

	// Reserved 10 bytes: all 0
	pos = misc.WriteZeroes(data, pos, 10)

	// Second part of auth plugin Content.
	pos += copy(data[pos:], salt[8:])
	data[pos] = 0
	pos++

	// Copy authPluginName. We always start with mysql_native_password.
	pos = misc.WriteNullString(data, pos, constant.MysqlNativePassword)

	// Sanity check.
	if pos != len(data) {
		return errors.Errorf("error building Handshake packet: got %v bytes expected %v", pos, len(data))
	}

	if err := c.WriteEphemeralPacket(); err != nil {
		if strings.HasSuffix(err.Error(), "write: connection reset by peer") {
			return io.EOF
		}
		if strings.HasSuffix(err.Error(), "write: broken pipe") {
			return io.EOF
		}
		return err
	}

	return nil
}

// parseClientHandshakePacket parses the handshake sent by the client.
// Returns the username, auth method, auth Content, error.
// The original Content is not pointed at, and can be freed.
func (l *MysqlListener) parseClientHandshakePacket(firstTime bool, data []byte) (string, string, []byte, error) {
	pos := 0

	// Client flags, 4 bytes.
	clientFlags, pos, ok := misc.ReadUint32(data, pos)
	if !ok {
		return "", "", nil, errors.Errorf("parseClientHandshakePacket: can't read client flags")
	}
	if clientFlags&constant.CapabilityClientProtocol41 == 0 {
		return "", "", nil, errors.Errorf("parseClientHandshakePacket: only support protocol 4.1")
	}

	// Remember a subset of the capabilities, so we can use them
	// later in the protocol. If we re-received the handshake packet
	// after SSL negotiation, do not overwrite capabilities.
	if firstTime {
		l.capabilities = clientFlags & (constant.CapabilityClientDeprecateEOF | constant.CapabilityClientFoundRows)
	}

	// set connection capability for executing multi statements
	if clientFlags&constant.CapabilityClientMultiStatements > 0 {
		l.capabilities |= constant.CapabilityClientMultiStatements
	}

	// Max packet size. Don't do anything with this now.
	// See doc.go for more information.
	_, pos, ok = misc.ReadUint32(data, pos)
	if !ok {
		return "", "", nil, errors.Errorf("parseClientHandshakePacket: can't read maxPacketSize")
	}

	// Character set. Need to handle it.
	characterSet, pos, ok := misc.ReadByte(data, pos)
	if !ok {
		return "", "", nil, errors.Errorf("parseClientHandshakePacket: can't read characterSet")
	}
	l.characterSet = characterSet

	// 23x reserved zero bytes.
	pos += 23

	//// Check for SSL.
	//if firstTime && l.TLSConfig != nil && clientFlags&CapabilityClientSSL > 0 {
	//	// Need to switch to TLS, and then re-read the packet.
	//	conn := tls.Server(c.conn, l.TLSConfig)
	//	c.conn = conn
	//	c.bufferedReader.Reset(conn)
	//	l.capabilities |= CapabilityClientSSL
	//	return "", "", nil, nil
	//}

	// username
	username, pos, ok := misc.ReadNullString(data, pos)
	if !ok {
		return "", "", nil, errors.Errorf("parseClientHandshakePacket: can't read username")
	}

	// auth-response can have three forms.
	var authResponse []byte
	if clientFlags&constant.CapabilityClientPluginAuthLenencClientData != 0 {
		var l uint64
		l, pos, ok = misc.ReadLenEncInt(data, pos)
		if !ok {
			return "", "", nil, errors.Errorf("parseClientHandshakePacket: can't read auth-response variable length")
		}
		authResponse, pos, ok = misc.ReadBytesCopy(data, pos, int(l))
		if !ok {
			return "", "", nil, errors.Errorf("parseClientHandshakePacket: can't read auth-response")
		}

	} else if clientFlags&constant.CapabilityClientSecureConnection != 0 {
		var l byte
		l, pos, ok = misc.ReadByte(data, pos)
		if !ok {
			return "", "", nil, errors.Errorf("parseClientHandshakePacket: can't read auth-response length")
		}

		authResponse, pos, ok = misc.ReadBytesCopy(data, pos, int(l))
		if !ok {
			return "", "", nil, errors.Errorf("parseClientHandshakePacket: can't read auth-response")
		}
	} else {
		a := ""
		a, pos, ok = misc.ReadNullString(data, pos)
		if !ok {
			return "", "", nil, errors.Errorf("parseClientHandshakePacket: can't read auth-response")
		}
		authResponse = []byte(a)
	}

	// db name.
	if clientFlags&constant.CapabilityClientConnectWithDB != 0 {
		dbname := ""
		dbname, pos, ok = misc.ReadNullString(data, pos)
		if !ok {
			return "", "", nil, errors.Errorf("parseClientHandshakePacket: can't read dbname")
		}
		l.schemaName = dbname
	}

	// authMethod (with default)
	authMethod := constant.MysqlNativePassword
	if clientFlags&constant.CapabilityClientPluginAuth != 0 {
		authMethod, pos, ok = misc.ReadNullString(data, pos)
		if !ok {
			return "", "", nil, errors.Errorf("parseClientHandshakePacket: can't read authMethod")
		}
	}

	// The JDBC driver sometimes sends an empty string as the auth method when it wants to use mysql_native_password
	if authMethod == "" {
		authMethod = constant.MysqlNativePassword
	}

	// Decode connection attributes send by the client
	if clientFlags&constant.CapabilityClientConnAttr != 0 {
		if _, _, err := parseConnAttrs(data, pos); err != nil {
			log.Warnf("Decode connection attributes send by the client: %v", err)
		}
	}

	return username, authMethod, authResponse, nil
}

func (l *MysqlListener) ValidateHash(user string, salt []byte, authResponse []byte) error {
	password, ok := l.conf.Users[user]
	if !ok {
		return err2.NewSQLError(constant.ERAccessDeniedError, constant.SSAccessDeniedError, "Access denied for user '%v'", user)
	}
	computedAuthResponse := scramblePassword(salt, password)
	if bytes.Equal(authResponse, computedAuthResponse) {
		return nil
	}
	return err2.NewSQLError(constant.ERAccessDeniedError, constant.SSAccessDeniedError, "Access denied for user '%v'", user)
}

// Hash password using 4.1+ method (SHA1)
func scramblePassword(scramble []byte, password string) []byte {
	if len(password) == 0 {
		return nil
	}

	// stage1Hash = SHA1(password)
	crypt := sha1.New()
	crypt.Write([]byte(password))
	stage1 := crypt.Sum(nil)

	// scrambleHash = SHA1(scramble + SHA1(stage1Hash))
	// inner Hash
	crypt.Reset()
	crypt.Write(stage1)
	hash := crypt.Sum(nil)

	// outer Hash
	crypt.Reset()
	crypt.Write(scramble)
	crypt.Write(hash)
	scramble = crypt.Sum(nil)

	// token = scrambleHash XOR stage1Hash
	for i := range scramble {
		scramble[i] ^= stage1[i]
	}
	return scramble
}

func (l *MysqlListener) ExecuteCommand(ctx context.Context, c *mysql.Conn, data []byte) error {
	commandType := data[0]
	switch commandType {
	case constant.ComQuit:
		// https://dev.constant.Com/doc/internals/en/com-quit.html
		c.RecycleReadPacket()
		connectionID := proto.ConnectionID(ctx)
		l.executor.ConnectionClose(proto.WithConnectionID(ctx, connectionID))
		log.Debugf("connection closed, id: %d", connectionID)
		return errors.New("ComQuit")
	case constant.ComInitDB:
		db := string(data[1:])
		c.RecycleReadPacket()
		l.schemaName = db
		err := l.executor.ExecuteUseDB(ctx, db)
		if err != nil {
			return err
		}
		if err := c.WriteOKPacket(0, 0, c.StatusFlags(), 0); err != nil {
			log.Errorf("Error writing ComInitDB result to %s: %v", c, err)
			return err
		}
	case constant.ComQuery:
		err := func() error {
			c.StartWriterBuffering()
			defer func() {
				if err := c.EndWriterBuffering(); err != nil {
					log.Errorf("conn %v: flush() failed: %v", c.ID(), err)
				}
			}()
			query := string(data[1:])
			c.RecycleReadPacket()
			p := parser.New()
			stmt, err := p.ParseOneStmt(query, "", "")
			if err != nil {
				if writeErr := c.WriteErrorPacketFromError(err); writeErr != nil {
					log.Error("Error writing query error to client %v: %v", l.connectionID, writeErr)
					return writeErr
				}
				return nil
			}

			traceCtx := tracing.BuildContextFromSQLHint(ctx, stmt)
			newCtx, span := tracing.GetTraceSpan(traceCtx, tracing.MySQLComQuery)
			defer span.End()

			stmt.Accept(&visitor.ParamVisitor{})
			ctx = proto.WithCommandType(newCtx, commandType)
			ctx = proto.WithQueryStmt(ctx, stmt)
			ctx = proto.WithSqlText(ctx, query)
			result, warn, err := l.executor.ExecutorComQuery(ctx, query)
			if err != nil {
				if writeErr := c.WriteErrorPacketFromError(err); writeErr != nil {
					log.Error("Error writing query error to client %v: %v", l.connectionID, writeErr)
					return writeErr
				}
				return nil
			}
			if rlt, ok := result.(*mysql.Result); ok {
				if len(rlt.Fields) == 0 {
					// A successful callback with no fields means that this was a
					// DML or other write-only operation.
					//
					// We should not send any more packets after this, but make sure
					// to extract the affected rows and last insert id from the result
					// struct here since clients expect it.
					flag := c.StatusFlags()
					if l.executor.InLocalTransaction(ctx) {
						flag = flag | constant.ServerStatusInTrans
					}
					return c.WriteOKPacket(rlt.AffectedRows, rlt.InsertId, flag, warn)
				}
				err = c.WriteFields(l.capabilities, rlt.Fields)
				if err != nil {
					tracing.RecordErrorSpan(span, err)
					return err
				}
				err = c.WriteTextRows(rlt)
				if err != nil {
					tracing.RecordErrorSpan(span, err)
					return err
				}
			}
			if rlt, ok := result.(*mysql.MergeResult); ok {
				if len(rlt.Fields) == 0 {
					// A successful callback with no fields means that this was a
					// DML or other write-only operation.
					//
					// We should not send any more packets after this, but make sure
					// to extract the affected rows and last insert id from the result
					// struct here since clients expect it.
					return c.WriteOKPacket(rlt.AffectedRows, rlt.InsertId, c.StatusFlags(), warn)
				}
				err = c.WriteFields(l.capabilities, rlt.Fields)
				if err != nil {
					tracing.RecordErrorSpan(span, err)
					return err
				}
				err = c.WriteRows(rlt)
				if err != nil {
					tracing.RecordErrorSpan(span, err)
					return err
				}
			}
			if err = c.WriteEndResult(l.capabilities, false, 0, 0, warn); err != nil {
				log.Errorf("Error writing result to %s: %v", c, err)
				tracing.RecordErrorSpan(span, err)
				return err
			}
			return nil
		}()
		if err != nil {
			return err
		}
	case constant.ComPing:
		c.RecycleReadPacket()

		// Return error if MysqlListener was shut down and OK otherwise
		if err := c.WriteOKPacket(0, 0, c.StatusFlags(), 0); err != nil {
			log.Errorf("Error writing ComPing result to %s: %v", c, err)
			return err
		}
	case constant.ComFieldList:
		index := bytes.IndexByte(data, 0x00)
		table := string(data[0:index])
		wildcard := string(data[index+1:])
		c.RecycleReadPacket()
		fields, err := l.executor.ExecuteFieldList(ctx, table, wildcard)
		if err != nil {
			log.Errorf("Conn %v: Error write field list: %v", c, err)
			if writeErr := c.WriteErrorPacketFromError(err); writeErr != nil {
				// If we can't even write the error, we're done.
				log.Errorf("Conn %v: Error write field list error: %v", c, writeErr)
				return writeErr
			}
		}
		result := &mysql.Result{Fields: make([]*mysql.Field, 0, len(fields))}
		for i, field := range fields {
			fld := field.(*mysql.Field)
			result.Fields[i] = fld
		}
		err = c.WriteFields(l.capabilities, result.Fields)
		if err != nil {
			return err
		}
	case constant.ComPrepare:
		query := string(data[1:])
		c.RecycleReadPacket()

		// Populate PrepareData
		l.statementID.Inc()
		stmt := &proto.Stmt{
			StatementID: l.statementID.Load(),
			SqlText:     query,
		}
		p := parser.New()
		act, err := p.ParseOneStmt(stmt.SqlText, "", "")

		if err != nil {
			log.Errorf("Conn %v: Error parsing prepared statement: %v", c, err)
			if writeErr := c.WriteErrorPacketFromError(err); writeErr != nil {
				// If we can't even write the error, we're done.
				log.Errorf("Conn %v: Error writing prepared statement error: %v", c, writeErr)
				return writeErr
			}
		}
		act.Accept(&visitor.ParamVisitor{})

		stmt.StmtNode = act

		paramsCount := uint16(strings.Count(query, "?"))

		if paramsCount > 0 {
			stmt.ParamsCount = paramsCount
			stmt.ParamsType = make([]int32, paramsCount)
			stmt.BindVars = make(map[string]interface{}, paramsCount)
		}

		l.stmts.Store(stmt.StatementID, stmt)

		if err = c.WritePrepare(l.capabilities, stmt); err != nil {
			return err
		}
	case constant.ComStmtExecute:
		err := func() error {
			c.StartWriterBuffering()
			defer func() {
				if err := c.EndWriterBuffering(); err != nil {
					log.Errorf("conn %v: flush() failed: %v", c.ID(), err)
				}
			}()
			stmtID, _, err := packet.ParseComStmtExecute(l.stmts, data)
			c.RecycleReadPacket()

			if stmtID != uint32(0) {
				defer func() {
					// Allocate a new bindvar map every time since executor.Execute() mutates it.
					pi, _ := l.stmts.Load(stmtID)
					prepare := pi.(*proto.Stmt)
					prepare.BindVars = make(map[string]interface{}, prepare.ParamsCount)
				}()
			}

			if err != nil {
				if writeErr := c.WriteErrorPacketFromError(err); writeErr != nil {
					// If we can't even write the error, we're done.
					log.Error("Error writing query error to client %v: %v", l.connectionID, writeErr)
					return writeErr
				}
				return nil
			}

			si, _ := l.stmts.Load(stmtID)
			stmt := si.(*proto.Stmt)
			stmt.ParamData = data

			traceCtx := tracing.BuildContextFromSQLHint(ctx, stmt.StmtNode)
			newCtx, span := tracing.GetTraceSpan(traceCtx, "mysql_com_stmt_execute")
			defer span.End()

			ctx = proto.WithCommandType(newCtx, commandType)
			ctx = proto.WithPrepareStmt(ctx, stmt)
			ctx = proto.WithSqlText(ctx, stmt.SqlText)
			result, warn, err := l.executor.ExecutorComStmtExecute(ctx, stmt)
			if err != nil {
				if writeErr := c.WriteErrorPacketFromError(err); writeErr != nil {
					log.Error("Error writing query error to client %v: %v", l.connectionID, writeErr)
					tracing.RecordErrorSpan(span, writeErr)
					return writeErr
				}
				return nil
			}
			if rlt, ok := result.(*mysql.Result); ok {
				if len(rlt.Fields) == 0 {
					// A successful callback with no fields means that this was a
					// DML or other write-only operation.
					//
					// We should not send any more packets after this, but make sure
					// to extract the affected rows and last insert id from the result
					// struct here since clients expect it.
					flag := c.StatusFlags()
					if l.executor.InLocalTransaction(ctx) {
						flag = flag | constant.ServerStatusInTrans
					}
					return c.WriteOKPacket(rlt.AffectedRows, rlt.InsertId, flag, warn)
				}

				err = c.WriteFields(l.capabilities, rlt.Fields)
				if err != nil {
					tracing.RecordErrorSpan(span, err)
					return err
				}
				err = c.WriteBinaryRows(rlt)
				if err != nil {
					tracing.RecordErrorSpan(span, err)
					return err
				}
			}
			if rlt, ok := result.(*mysql.MergeResult); ok {
				if len(rlt.Fields) == 0 {
					// A successful callback with no fields means that this was a
					// DML or other write-only operation.
					//
					// We should not send any more packets after this, but make sure
					// to extract the affected rows and last insert id from the result
					// struct here since clients expect it.
					return c.WriteOKPacket(rlt.AffectedRows, rlt.InsertId, c.StatusFlags(), warn)
				}
				err = c.WriteFields(l.capabilities, rlt.Fields)
				if err != nil {
					tracing.RecordErrorSpan(span, err)
					return err
				}
				err = c.WriteRows(rlt)
				if err != nil {
					tracing.RecordErrorSpan(span, err)
					return err
				}
			}
			if err = c.WriteEndResult(l.capabilities, false, 0, 0, warn); err != nil {
				log.Errorf("Error writing result to %s: %v", c, err)
				tracing.RecordErrorSpan(span, err)
				return err
			}
			return nil
		}()
		if err != nil {
			return err
		}
	case constant.ComStmtClose: // no response
		stmtID, _, ok := misc.ReadUint32(data, 1)
		c.RecycleReadPacket()
		if ok {
			l.stmts.Delete(stmtID)
		}
	case constant.ComStmtSendLongData: // no response
		if len(data) < 6 {
			return err2.ErrMalformedPkt
		}

		stmtID := int(binary.LittleEndian.Uint32(data[0:4]))

		si, ok := l.stmts.Load(stmtID)
		if !ok {
			return errors.Errorf("statement not found, statement id: %v", stmtID)
		}
		stmt := si.(*proto.Stmt)

		paramID := int(binary.LittleEndian.Uint16(data[4:6]))
		parameterID := fmt.Sprintf("v%d", paramID+1)
		stmt.HasLongDataParam = true
		stmt.BindVars[parameterID] = data[6:]
	case constant.ComStmtReset:
		stmtID, _, ok := misc.ReadUint32(data, 1)
		c.RecycleReadPacket()
		if ok {
			si, _ := l.stmts.Load(stmtID)
			stmt := si.(*proto.Stmt)
			stmt.BindVars = make(map[string]interface{}, 0)
		}
		return c.WriteOKPacket(0, 0, c.StatusFlags(), 0)
	case constant.ComSetOption:
		operation, _, ok := misc.ReadUint16(data, 1)
		c.RecycleReadPacket()
		if ok {
			switch operation {
			case 0:
				l.capabilities |= constant.CapabilityClientMultiStatements
			case 1:
				l.capabilities &^= constant.CapabilityClientMultiStatements
			default:
				log.Errorf("Got unhandled packet (ComSetOption default) from client %v, returning error: %v", l.connectionID, data)
				if err := c.WriteErrorPacket(constant.ERUnknownComError, constant.SSUnknownComError, "error handling packet: %v", data); err != nil {
					log.Errorf("Error writing error packet to client: %v", err)
					return err
				}
			}
			if err := c.WriteEndResult(l.capabilities, false, 0, 0, 0); err != nil {
				log.Errorf("Error writeEndResult error %v ", err)
				return err
			}
		} else {
			log.Errorf("Got unhandled packet (ComSetOption else) from client %v, returning error: %v", l.connectionID, data)
			if err := c.WriteErrorPacket(constant.ERUnknownComError, constant.SSUnknownComError, "error handling packet: %v", data); err != nil {
				log.Errorf("Error writing error packet to client: %v", err)
				return err
			}
		}
	}
	return nil
}

func parseConnAttrs(data []byte, pos int) (map[string]string, int, error) {
	var attrLen uint64

	attrLen, pos, ok := misc.ReadLenEncInt(data, pos)
	if !ok {
		return nil, 0, errors.Errorf("parseClientHandshakePacket: can't read connection attributes variable length")
	}

	var attrLenRead uint64

	attrs := make(map[string]string)

	for attrLenRead < attrLen {
		var keyLen byte
		keyLen, pos, ok = misc.ReadByte(data, pos)
		if !ok {
			return nil, 0, errors.Errorf("parseClientHandshakePacket: can't read connection attribute key length")
		}
		attrLenRead += uint64(keyLen) + 1

		var connAttrKey []byte
		connAttrKey, pos, ok = misc.ReadBytesCopy(data, pos, int(keyLen))
		if !ok {
			return nil, 0, errors.Errorf("parseClientHandshakePacket: can't read connection attribute key")
		}

		var valLen byte
		valLen, pos, ok = misc.ReadByte(data, pos)
		if !ok {
			return nil, 0, errors.Errorf("parseClientHandshakePacket: can't read connection attribute value length")
		}
		attrLenRead += uint64(valLen) + 1

		var connAttrVal []byte
		connAttrVal, pos, ok = misc.ReadBytesCopy(data, pos, int(valLen))
		if !ok {
			return nil, 0, errors.Errorf("parseClientHandshakePacket: can't read connection attribute value")
		}

		attrs[string(connAttrKey[:])] = string(connAttrVal[:])
	}

	return attrs, pos, nil
}

// newSalt returns a 20 character salt.
func newSalt() ([]byte, error) {
	salt := make([]byte, 20)
	if _, err := rand.Read(salt); err != nil {
		return nil, err
	}

	// Salt must be a legal UTF8 string.
	for i := 0; i < len(salt); i++ {
		salt[i] &= 0x7f
		if salt[i] == '\x00' || salt[i] == '$' {
			salt[i]++
		}
	}

	return salt, nil
}
