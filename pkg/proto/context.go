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

package proto

import (
	"context"

	"github.com/cectc/dbpack/third_party/parser/ast"
)

const (
	_flagMaster cFlag = 1 << iota
	_flagSlave
)

type (
	keyFlag         struct{}
	keyConnectionID struct{}
	keyUserName     struct{}
	keySchema       struct{}
	keyCommandType  struct{}
	keyQueryStmt    struct{}
	keyPrepareStmt  struct{}
	keyVariableMap  struct{}
	keySqlText      struct{}
	keyRemoteAddr   struct{}
)

type cFlag uint8

// WithMaster uses master datasource.
func WithMaster(ctx context.Context) context.Context {
	return context.WithValue(ctx, keyFlag{}, _flagMaster|getFlag(ctx))
}

// WithSlave uses slave datasource.
func WithSlave(ctx context.Context) context.Context {
	return context.WithValue(ctx, keyFlag{}, _flagSlave|getFlag(ctx))
}

// IsMaster returns true if force using master.
func IsMaster(ctx context.Context) bool {
	return hasFlag(ctx, _flagMaster)
}

// IsSlave returns true if force using master.
func IsSlave(ctx context.Context) bool {
	return hasFlag(ctx, _flagSlave)
}

// WithConnectionID binds connection id
func WithConnectionID(ctx context.Context, connectionID uint32) context.Context {
	return context.WithValue(ctx, keyConnectionID{}, connectionID)
}

// ConnectionID extracts connection id
func ConnectionID(ctx context.Context) uint32 {
	connectionID, ok := ctx.Value(keyConnectionID{}).(uint32)
	if ok {
		return connectionID
	}
	return 0
}

// WithUserName binds user name
func WithUserName(ctx context.Context, userName string) context.Context {
	return context.WithValue(ctx, keyUserName{}, userName)
}

// UserName extracts user name
func UserName(ctx context.Context) string {
	userID, ok := ctx.Value(keyUserName{}).(string)
	if ok {
		return userID
	}
	return ""
}

// WithSchema binds schema
func WithSchema(ctx context.Context, schema string) context.Context {
	return context.WithValue(ctx, keySchema{}, schema)
}

// Schema extracts schema
func Schema(ctx context.Context) string {
	schema, ok := ctx.Value(keySchema{}).(string)
	if ok {
		return schema
	}
	return ""
}

// WithCommandType binds command type
func WithCommandType(ctx context.Context, commandType byte) context.Context {
	return context.WithValue(ctx, keyCommandType{}, commandType)
}

// CommandType extracts command type
func CommandType(ctx context.Context) byte {
	commandType, ok := ctx.Value(keyCommandType{}).(byte)
	if ok {
		return commandType
	}
	return 0
}

// WithQueryStmt binds query stmt
func WithQueryStmt(ctx context.Context, stmt ast.StmtNode) context.Context {
	return context.WithValue(ctx, keyQueryStmt{}, stmt)
}

// QueryStmt extracts query stmt
func QueryStmt(ctx context.Context) ast.StmtNode {
	stmt, ok := ctx.Value(keyQueryStmt{}).(ast.StmtNode)
	if ok {
		return stmt
	}
	return nil
}

// WithPrepareStmt binds prepare stmt
func WithPrepareStmt(ctx context.Context, stmt *Stmt) context.Context {
	return context.WithValue(ctx, keyPrepareStmt{}, stmt)
}

// PrepareStmt extracts prepare stmt
func PrepareStmt(ctx context.Context) *Stmt {
	stmt, ok := ctx.Value(keyPrepareStmt{}).(*Stmt)
	if ok {
		return stmt
	}
	return nil
}

// WithVariableMap binds a map[string]interface{} to store temp variable
func WithVariableMap(ctx context.Context) context.Context {
	return context.WithValue(ctx, keyVariableMap{}, make(map[string]interface{}, 0))
}

// WithVariable binds a key value pair
func WithVariable(ctx context.Context, key string, value interface{}) bool {
	variables, ok := ctx.Value(keyVariableMap{}).(map[string]interface{})
	if ok {
		variables[key] = value
		return true
	}
	return false
}

// Variable extracts variable value
func Variable(ctx context.Context, key string) interface{} {
	variables, ok := ctx.Value(keyVariableMap{}).(map[string]interface{})
	if ok {
		return variables[key]
	}
	return nil
}

// WithSqlText binds sql text
func WithSqlText(ctx context.Context, sqlText string) context.Context {
	return context.WithValue(ctx, keySqlText{}, sqlText)
}

// SqlText extracts sql text
func SqlText(ctx context.Context) string {
	sqlText, ok := ctx.Value(keySqlText{}).(string)
	if ok {
		return sqlText
	}
	return ""
}

// WithRemoteAddr binds remote address
func WithRemoteAddr(ctx context.Context, remoteAddr string) context.Context {
	return context.WithValue(ctx, keyRemoteAddr{}, remoteAddr)
}

// RemoteAddr extracts remote addr
func RemoteAddr(ctx context.Context) string {
	remoteAddr, ok := ctx.Value(keyRemoteAddr{}).(string)
	if ok {
		return remoteAddr
	}
	return ""
}

func hasFlag(ctx context.Context, flag cFlag) bool {
	return getFlag(ctx)&flag != 0
}

func getFlag(ctx context.Context) cFlag {
	f, ok := ctx.Value(keyFlag{}).(cFlag)
	if !ok {
		return 0
	}
	return f
}
