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

package metrics

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/cectc/dbpack/pkg/constant"
	"github.com/cectc/dbpack/pkg/filter"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/third_party/parser/ast"
)

const (
	connectionMetricFilterName = "ConnectionMetricFilter"
)

func init() {
	filter.RegistryFilterFactory(connectionMetricFilterName, &connectionMetricFactory{})
}

type connectionMetricFactory struct{}

func (factory *connectionMetricFactory) NewFilter(config map[string]interface{}) (proto.Filter, error) {
	connectionFilterExecDuration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dbpack",
			Subsystem: "connection",
			Name:      "execute_latency",
			Help:      "The time it took to execute filter for mysql",
			Buckets:   prometheus.ExponentialBuckets(0.001 /* 1 ms */, 2, 18),
		}, []string{"database", "command_type", "command"})
	prometheus.MustRegister(connectionFilterExecDuration)
	return &_connectionMetricFilter{
		connectionFilterExecDuration: connectionFilterExecDuration, timeKey: "start_at"}, nil
}

type _connectionMetricFilter struct {
	connectionFilterExecDuration *prometheus.HistogramVec
	timeKey                      string
}

func (f *_connectionMetricFilter) GetName() string {
	return connectionMetricFilterName
}

func (f *_connectionMetricFilter) PreHandle(ctx context.Context, conn proto.Connection) error {
	start := time.Now()
	conn.DataSourceName()
	proto.WithVariable(ctx, f.timeKey, start)
	return nil
}

func (f *_connectionMetricFilter) PostHandle(ctx context.Context, result proto.Result, conn proto.Connection) error {
	v := proto.Variable(ctx, f.timeKey)
	if startAt, ok := v.(time.Time); ok {
		stmt := proto.PrepareStmt(ctx)
		commandType := proto.CommandType(ctx)
		var command string
		var strCommandType string
		switch stmt.StmtNode.(type) {
		case *ast.DeleteStmt:
			command = "delete"
		case *ast.InsertStmt:
			command = "insert"
		case *ast.UpdateStmt:
			command = "update"
		case *ast.SelectStmt:
			command = "select"
		}

		switch commandType {
		case constant.ComQuery:
			strCommandType = "com_query"
		case constant.ComStmtExecute:
			strCommandType = "com_stmt_execute"
		}

		if len(strCommandType) > 0 && len(command) > 0 {
			f.connectionFilterExecDuration.WithLabelValues(conn.DataSourceName(), strCommandType, command).Observe(time.Since(startAt).Seconds())
		} else {
			log.Warnf("not support command_type %v and not support command %s", commandType, stmt.StmtNode.Text())
		}
	}
	return nil
}
