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
// Source: github.com/cectc/dbpack/pkg/proto (interfaces: DB)

// Package testdata is a generated GoMock package.
package testdata

import (
	context "context"
	reflect "reflect"
	time "time"

	gomock "github.com/golang/mock/gomock"

	proto "github.com/cectc/dbpack/pkg/proto"
)

// MockDB is a mock of DB interface.
type MockDB struct {
	ctrl     *gomock.Controller
	recorder *MockDBMockRecorder
}

// MockDBMockRecorder is the mock recorder for MockDB.
type MockDBMockRecorder struct {
	mock *MockDB
}

// NewMockDB creates a new mock instance.
func NewMockDB(ctrl *gomock.Controller) *MockDB {
	mock := &MockDB{ctrl: ctrl}
	mock.recorder = &MockDBMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockDB) EXPECT() *MockDBMockRecorder {
	return m.recorder
}

// Active mocks base method.
func (m *MockDB) Active() int64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Active")
	ret0, _ := ret[0].(int64)
	return ret0
}

// Active indicates an expected call of Active.
func (mr *MockDBMockRecorder) Active() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Active", reflect.TypeOf((*MockDB)(nil).Active))
}

// Available mocks base method.
func (m *MockDB) Available() int64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Available")
	ret0, _ := ret[0].(int64)
	return ret0
}

// Available indicates an expected call of Available.
func (mr *MockDBMockRecorder) Available() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Available", reflect.TypeOf((*MockDB)(nil).Available))
}

// Begin mocks base method.
func (m *MockDB) Begin(arg0 context.Context) (proto.Tx, proto.Result, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Begin", arg0)
	ret0, _ := ret[0].(proto.Tx)
	ret1, _ := ret[1].(proto.Result)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// Begin indicates an expected call of Begin.
func (mr *MockDBMockRecorder) Begin(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Begin", reflect.TypeOf((*MockDB)(nil).Begin), arg0)
}

// Capacity mocks base method.
func (m *MockDB) Capacity() int64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Capacity")
	ret0, _ := ret[0].(int64)
	return ret0
}

// Capacity indicates an expected call of Capacity.
func (mr *MockDBMockRecorder) Capacity() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Capacity", reflect.TypeOf((*MockDB)(nil).Capacity))
}

// Close mocks base method.
func (m *MockDB) Close() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Close")
}

// Close indicates an expected call of Close.
func (mr *MockDBMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockDB)(nil).Close))
}

// ExecuteFieldList mocks base method.
func (m *MockDB) ExecuteFieldList(arg0 context.Context, arg1, arg2 string) ([]proto.Field, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ExecuteFieldList", arg0, arg1, arg2)
	ret0, _ := ret[0].([]proto.Field)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ExecuteFieldList indicates an expected call of ExecuteFieldList.
func (mr *MockDBMockRecorder) ExecuteFieldList(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ExecuteFieldList", reflect.TypeOf((*MockDB)(nil).ExecuteFieldList), arg0, arg1, arg2)
}

// ExecuteSql mocks base method.
func (m *MockDB) ExecuteSql(arg0 context.Context, arg1 string, arg2 ...interface{}) (proto.Result, uint16, error) {
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
func (mr *MockDBMockRecorder) ExecuteSql(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ExecuteSql", reflect.TypeOf((*MockDB)(nil).ExecuteSql), varargs...)
}

// ExecuteSqlDirectly mocks base method.
func (m *MockDB) ExecuteSqlDirectly(arg0 string, arg1 ...interface{}) (proto.Result, uint16, error) {
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
func (mr *MockDBMockRecorder) ExecuteSqlDirectly(arg0 interface{}, arg1 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0}, arg1...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ExecuteSqlDirectly", reflect.TypeOf((*MockDB)(nil).ExecuteSqlDirectly), varargs...)
}

// ExecuteStmt mocks base method.
func (m *MockDB) ExecuteStmt(arg0 context.Context, arg1 *proto.Stmt) (proto.Result, uint16, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ExecuteStmt", arg0, arg1)
	ret0, _ := ret[0].(proto.Result)
	ret1, _ := ret[1].(uint16)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// ExecuteStmt indicates an expected call of ExecuteStmt.
func (mr *MockDBMockRecorder) ExecuteStmt(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ExecuteStmt", reflect.TypeOf((*MockDB)(nil).ExecuteStmt), arg0, arg1)
}

// Exhausted mocks base method.
func (m *MockDB) Exhausted() int64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Exhausted")
	ret0, _ := ret[0].(int64)
	return ret0
}

// Exhausted indicates an expected call of Exhausted.
func (mr *MockDBMockRecorder) Exhausted() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Exhausted", reflect.TypeOf((*MockDB)(nil).Exhausted))
}

// IdleClosed mocks base method.
func (m *MockDB) IdleClosed() int64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IdleClosed")
	ret0, _ := ret[0].(int64)
	return ret0
}

// IdleClosed indicates an expected call of IdleClosed.
func (mr *MockDBMockRecorder) IdleClosed() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IdleClosed", reflect.TypeOf((*MockDB)(nil).IdleClosed))
}

// IdleTimeout mocks base method.
func (m *MockDB) IdleTimeout() time.Duration {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IdleTimeout")
	ret0, _ := ret[0].(time.Duration)
	return ret0
}

// IdleTimeout indicates an expected call of IdleTimeout.
func (mr *MockDBMockRecorder) IdleTimeout() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IdleTimeout", reflect.TypeOf((*MockDB)(nil).IdleTimeout))
}

// InUse mocks base method.
func (m *MockDB) InUse() int64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "InUse")
	ret0, _ := ret[0].(int64)
	return ret0
}

// InUse indicates an expected call of InUse.
func (mr *MockDBMockRecorder) InUse() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "InUse", reflect.TypeOf((*MockDB)(nil).InUse))
}

// IsClosed mocks base method.
func (m *MockDB) IsClosed() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IsClosed")
	ret0, _ := ret[0].(bool)
	return ret0
}

// IsClosed indicates an expected call of IsClosed.
func (mr *MockDBMockRecorder) IsClosed() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IsClosed", reflect.TypeOf((*MockDB)(nil).IsClosed))
}

// IsMaster mocks base method.
func (m *MockDB) IsMaster() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IsMaster")
	ret0, _ := ret[0].(bool)
	return ret0
}

// IsMaster indicates an expected call of IsMaster.
func (mr *MockDBMockRecorder) IsMaster() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IsMaster", reflect.TypeOf((*MockDB)(nil).IsMaster))
}

// MasterName mocks base method.
func (m *MockDB) MasterName() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "MasterName")
	ret0, _ := ret[0].(string)
	return ret0
}

// MasterName indicates an expected call of MasterName.
func (mr *MockDBMockRecorder) MasterName() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "MasterName", reflect.TypeOf((*MockDB)(nil).MasterName))
}

// MaxCap mocks base method.
func (m *MockDB) MaxCap() int64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "MaxCap")
	ret0, _ := ret[0].(int64)
	return ret0
}

// MaxCap indicates an expected call of MaxCap.
func (mr *MockDBMockRecorder) MaxCap() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "MaxCap", reflect.TypeOf((*MockDB)(nil).MaxCap))
}

// Name mocks base method.
func (m *MockDB) Name() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Name")
	ret0, _ := ret[0].(string)
	return ret0
}

// Name indicates an expected call of Name.
func (mr *MockDBMockRecorder) Name() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Name", reflect.TypeOf((*MockDB)(nil).Name))
}

// Ping mocks base method.
func (m *MockDB) Ping() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Ping")
	ret0, _ := ret[0].(error)
	return ret0
}

// Ping indicates an expected call of Ping.
func (mr *MockDBMockRecorder) Ping() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Ping", reflect.TypeOf((*MockDB)(nil).Ping))
}

// Query mocks base method.
func (m *MockDB) Query(arg0 context.Context, arg1 string) (proto.Result, uint16, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Query", arg0, arg1)
	ret0, _ := ret[0].(proto.Result)
	ret1, _ := ret[1].(uint16)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// Query indicates an expected call of Query.
func (mr *MockDBMockRecorder) Query(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Query", reflect.TypeOf((*MockDB)(nil).Query), arg0, arg1)
}

// QueryDirectly mocks base method.
func (m *MockDB) QueryDirectly(arg0 string) (proto.Result, uint16, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "QueryDirectly", arg0)
	ret0, _ := ret[0].(proto.Result)
	ret1, _ := ret[1].(uint16)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// QueryDirectly indicates an expected call of QueryDirectly.
func (mr *MockDBMockRecorder) QueryDirectly(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryDirectly", reflect.TypeOf((*MockDB)(nil).QueryDirectly), arg0)
}

// ReadWeight mocks base method.
func (m *MockDB) ReadWeight() int {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReadWeight")
	ret0, _ := ret[0].(int)
	return ret0
}

// ReadWeight indicates an expected call of ReadWeight.
func (mr *MockDBMockRecorder) ReadWeight() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReadWeight", reflect.TypeOf((*MockDB)(nil).ReadWeight))
}

// SetCapacity mocks base method.
func (m *MockDB) SetCapacity(arg0 int) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetCapacity", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetCapacity indicates an expected call of SetCapacity.
func (mr *MockDBMockRecorder) SetCapacity(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetCapacity", reflect.TypeOf((*MockDB)(nil).SetCapacity), arg0)
}

// SetConnectionPostFilters mocks base method.
func (m *MockDB) SetConnectionPostFilters(arg0 []proto.DBConnectionPostFilter) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetConnectionPostFilters", arg0)
}

// SetConnectionPostFilters indicates an expected call of SetConnectionPostFilters.
func (mr *MockDBMockRecorder) SetConnectionPostFilters(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetConnectionPostFilters", reflect.TypeOf((*MockDB)(nil).SetConnectionPostFilters), arg0)
}

// SetConnectionPreFilters mocks base method.
func (m *MockDB) SetConnectionPreFilters(arg0 []proto.DBConnectionPreFilter) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetConnectionPreFilters", arg0)
}

// SetConnectionPreFilters indicates an expected call of SetConnectionPreFilters.
func (mr *MockDBMockRecorder) SetConnectionPreFilters(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetConnectionPreFilters", reflect.TypeOf((*MockDB)(nil).SetConnectionPreFilters), arg0)
}

// SetIdleTimeout mocks base method.
func (m *MockDB) SetIdleTimeout(arg0 time.Duration) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetIdleTimeout", arg0)
}

// SetIdleTimeout indicates an expected call of SetIdleTimeout.
func (mr *MockDBMockRecorder) SetIdleTimeout(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetIdleTimeout", reflect.TypeOf((*MockDB)(nil).SetIdleTimeout), arg0)
}

// SetReadWeight mocks base method.
func (m *MockDB) SetReadWeight(arg0 int) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetReadWeight", arg0)
}

// SetReadWeight indicates an expected call of SetReadWeight.
func (mr *MockDBMockRecorder) SetReadWeight(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetReadWeight", reflect.TypeOf((*MockDB)(nil).SetReadWeight), arg0)
}

// SetWriteWeight mocks base method.
func (m *MockDB) SetWriteWeight(arg0 int) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetWriteWeight", arg0)
}

// SetWriteWeight indicates an expected call of SetWriteWeight.
func (mr *MockDBMockRecorder) SetWriteWeight(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetWriteWeight", reflect.TypeOf((*MockDB)(nil).SetWriteWeight), arg0)
}

// StatsJSON mocks base method.
func (m *MockDB) StatsJSON() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StatsJSON")
	ret0, _ := ret[0].(string)
	return ret0
}

// StatsJSON indicates an expected call of StatsJSON.
func (mr *MockDBMockRecorder) StatsJSON() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StatsJSON", reflect.TypeOf((*MockDB)(nil).StatsJSON))
}

// Status mocks base method.
func (m *MockDB) Status() proto.DBStatus {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Status")
	ret0, _ := ret[0].(proto.DBStatus)
	return ret0
}

// Status indicates an expected call of Status.
func (mr *MockDBMockRecorder) Status() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Status", reflect.TypeOf((*MockDB)(nil).Status))
}

// UseDB mocks base method.
func (m *MockDB) UseDB(arg0 context.Context, arg1 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UseDB", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// UseDB indicates an expected call of UseDB.
func (mr *MockDBMockRecorder) UseDB(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UseDB", reflect.TypeOf((*MockDB)(nil).UseDB), arg0, arg1)
}

// WaitCount mocks base method.
func (m *MockDB) WaitCount() int64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "WaitCount")
	ret0, _ := ret[0].(int64)
	return ret0
}

// WaitCount indicates an expected call of WaitCount.
func (mr *MockDBMockRecorder) WaitCount() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WaitCount", reflect.TypeOf((*MockDB)(nil).WaitCount))
}

// WaitTime mocks base method.
func (m *MockDB) WaitTime() time.Duration {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "WaitTime")
	ret0, _ := ret[0].(time.Duration)
	return ret0
}

// WaitTime indicates an expected call of WaitTime.
func (mr *MockDBMockRecorder) WaitTime() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WaitTime", reflect.TypeOf((*MockDB)(nil).WaitTime))
}

// WriteWeight mocks base method.
func (m *MockDB) WriteWeight() int {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "WriteWeight")
	ret0, _ := ret[0].(int)
	return ret0
}

// WriteWeight indicates an expected call of WriteWeight.
func (mr *MockDBMockRecorder) WriteWeight() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WriteWeight", reflect.TypeOf((*MockDB)(nil).WriteWeight))
}
