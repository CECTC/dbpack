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

// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/cectc/dbpack/pkg/proto (interfaces: Tx)

// Package testdata is a generated GoMock package.
package testdata

import (
	context "context"
	reflect "reflect"

	proto "github.com/cectc/dbpack/pkg/proto"
	ast "github.com/cectc/dbpack/third_party/parser/ast"
	gomock "github.com/golang/mock/gomock"
)

// MockTx is a mock of Tx interface.
type MockTx struct {
	ctrl     *gomock.Controller
	recorder *MockTxMockRecorder
}

// MockTxMockRecorder is the mock recorder for MockTx.
type MockTxMockRecorder struct {
	mock *MockTx
}

// NewMockTx creates a new mock instance.
func NewMockTx(ctrl *gomock.Controller) *MockTx {
	mock := &MockTx{ctrl: ctrl}
	mock.recorder = &MockTxMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockTx) EXPECT() *MockTxMockRecorder {
	return m.recorder
}

// Commit mocks base method.
func (m *MockTx) Commit(arg0 context.Context) (proto.Result, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Commit", arg0)
	ret0, _ := ret[0].(proto.Result)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Commit indicates an expected call of Commit.
func (mr *MockTxMockRecorder) Commit(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Commit", reflect.TypeOf((*MockTx)(nil).Commit), arg0)
}

// ExecuteSql mocks base method.
func (m *MockTx) ExecuteSql(arg0 context.Context, arg1 string, arg2 ...interface{}) (proto.Result, uint16, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "ExecuteSql", varargs...)
	ret0, _ := ret[0].(proto.Result)
	ret1, _ := ret[1].(uint16)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// ExecuteSql indicates an expected call of ExecuteSql.
func (mr *MockTxMockRecorder) ExecuteSql(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ExecuteSql", reflect.TypeOf((*MockTx)(nil).ExecuteSql), varargs...)
}

// ExecuteSqlDirectly mocks base method.
func (m *MockTx) ExecuteSqlDirectly(arg0 string, arg1 ...interface{}) (proto.Result, uint16, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0}
	for _, a := range arg1 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "ExecuteSqlDirectly", varargs...)
	ret0, _ := ret[0].(proto.Result)
	ret1, _ := ret[1].(uint16)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// ExecuteSqlDirectly indicates an expected call of ExecuteSqlDirectly.
func (mr *MockTxMockRecorder) ExecuteSqlDirectly(arg0 interface{}, arg1 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0}, arg1...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ExecuteSqlDirectly", reflect.TypeOf((*MockTx)(nil).ExecuteSqlDirectly), varargs...)
}

// ExecuteStmt mocks base method.
func (m *MockTx) ExecuteStmt(arg0 context.Context, arg1 *proto.Stmt) (proto.Result, uint16, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ExecuteStmt", arg0, arg1)
	ret0, _ := ret[0].(proto.Result)
	ret1, _ := ret[1].(uint16)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// ExecuteStmt indicates an expected call of ExecuteStmt.
func (mr *MockTxMockRecorder) ExecuteStmt(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ExecuteStmt", reflect.TypeOf((*MockTx)(nil).ExecuteStmt), arg0, arg1)
}

// Query mocks base method.
func (m *MockTx) Query(arg0 context.Context, arg1 string) (proto.Result, uint16, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Query", arg0, arg1)
	ret0, _ := ret[0].(proto.Result)
	ret1, _ := ret[1].(uint16)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// Query indicates an expected call of Query.
func (mr *MockTxMockRecorder) Query(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Query", reflect.TypeOf((*MockTx)(nil).Query), arg0, arg1)
}

// QueryDirectly mocks base method.
func (m *MockTx) QueryDirectly(arg0 string) (proto.Result, uint16, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "QueryDirectly", arg0)
	ret0, _ := ret[0].(proto.Result)
	ret1, _ := ret[1].(uint16)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// QueryDirectly indicates an expected call of QueryDirectly.
func (mr *MockTxMockRecorder) QueryDirectly(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryDirectly", reflect.TypeOf((*MockTx)(nil).QueryDirectly), arg0)
}

// Rollback mocks base method.
func (m *MockTx) Rollback(arg0 context.Context, arg1 *ast.RollbackStmt) (proto.Result, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Rollback", arg0, arg1)
	ret0, _ := ret[0].(proto.Result)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Rollback indicates an expected call of Rollback.
func (mr *MockTxMockRecorder) Rollback(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Rollback", reflect.TypeOf((*MockTx)(nil).Rollback), arg0, arg1)
}
