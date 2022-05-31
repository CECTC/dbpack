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

package executor

import (
	"context"

	"github.com/cectc/dbpack/pkg/lb"
	"github.com/cectc/dbpack/pkg/proto"
)

type DBGroupExecutor struct {
	dbGroupName string
	masters     lb.Interface
	reads       lb.Interface
}

func (ex *DBGroupExecutor) Begin(ctx context.Context) (proto.Tx, proto.Result, error) {
	db := ex.masters.Next(proto.WithMaster(ctx)).(*DataSourceBrief)
	return db.DB.Begin(ctx)
}

func (ex *DBGroupExecutor) Query(ctx context.Context, query string) (proto.Result, uint16, error) {
	db := ex.reads.Next(proto.WithSlave(ctx)).(*DataSourceBrief)
	return db.DB.Query(proto.WithSlave(ctx), query)
}

func (ex *DBGroupExecutor) Execute(ctx context.Context, query string) (proto.Result, uint16, error) {
	db := ex.masters.Next(proto.WithMaster(ctx)).(*DataSourceBrief)
	return db.DB.Query(proto.WithMaster(ctx), query)
}

func (ex *DBGroupExecutor) PrepareQuery(ctx context.Context, query string, args ...interface{}) (proto.Result, uint16, error) {
	db := ex.reads.Next(proto.WithSlave(ctx)).(*DataSourceBrief)
	return db.DB.ExecuteSql(proto.WithSlave(ctx), query, args...)
}

func (ex *DBGroupExecutor) PrepareExecute(ctx context.Context, query string, args ...interface{}) (proto.Result, uint16, error) {
	db := ex.masters.Next(proto.WithMaster(ctx)).(*DataSourceBrief)
	return db.DB.ExecuteSql(proto.WithMaster(ctx), query, args...)
}
