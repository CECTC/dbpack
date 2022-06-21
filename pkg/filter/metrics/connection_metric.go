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
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/third_party/parser/ast"
)

const (
	connectionMetricFilter = "ConnectionMetricFilter"
)

type _factory struct{}

func (factory *_factory) NewFilter(config map[string]interface{}) (proto.Filter, error) {
	connectionFilterExecDuration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dbpack",
			Subsystem: "connection",
			Name:      "execute_latency",
			Help:      "The time it took to execute filter for mysql",
			Buckets:   prometheus.ExponentialBuckets(0.001 /* 1 ms */, 2, 18),
		}, []string{"database", "command_type", "command"})
	prometheus.MustRegister(connectionFilterExecDuration)
	return &_filter{
		connectionFilterExecDuration: connectionFilterExecDuration, timeKey: "start_at"}, nil
}

type _filter struct {
	connectionFilterExecDuration *prometheus.HistogramVec
	timeKey                      string
}

func (f *_filter) GetKind() string {
	return connectionMetricFilter
}

func (f *_filter) PreHandle(ctx context.Context, conn proto.Connection) error {
	start := time.Now()
	proto.WithVariable(ctx, f.timeKey, start)
	return nil
}

func (f *_filter) PostHandle(ctx context.Context, result proto.Result, conn proto.Connection) error {
	v := proto.Variable(ctx, f.timeKey)
	startAt, ok := v.(time.Time)
	if !ok {
		return nil
	}

	commandType := proto.CommandType(ctx)

	var command string
	var commandTypeStr string
	var stmtNode ast.StmtNode

	switch commandType {
	case constant.ComQuery:
		commandTypeStr = "COM_QUERY"
		stmtNode = proto.QueryStmt(ctx)
	case constant.ComStmtExecute:
		commandTypeStr = "COM_STMT_EXECUTE"
		statement := proto.PrepareStmt(ctx)
		stmtNode = statement.StmtNode
	default:
		return nil
	}

	switch stmtNode.(type) {
	case *ast.DeleteStmt:
		command = "DELETE"
	case *ast.InsertStmt:
		command = "INSERT"
	case *ast.UpdateStmt:
		command = "UPDATE"
	case *ast.SelectStmt:
		command = "SELECT"
	default:
		return nil
	}

	f.connectionFilterExecDuration.WithLabelValues(conn.DataSourceName(), commandTypeStr, command).Observe(time.Since(startAt).Seconds())
	return nil
}

func init() {
	filter.RegistryFilterFactory(connectionMetricFilter, &_factory{})
}
