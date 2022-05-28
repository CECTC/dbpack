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

package exec

import (
	"context"
	"time"

	"github.com/cectc/dbpack/pkg/constant"
	"github.com/cectc/dbpack/pkg/dt/schema"
	"github.com/cectc/dbpack/pkg/dt/undolog"
)

type Executor interface {
	BeforeImage(ctx context.Context) (*schema.TableRecords, error)
	AfterImage(ctx context.Context) (*schema.TableRecords, error)
	GetTableMeta(ctx context.Context) (schema.TableMeta, error)
	GetTableName() string
}

type Executable interface {
	Executable(ctx context.Context, lockRetryInterval time.Duration, lockRetryTimes int) (bool, error)
	GetTableMeta(ctx context.Context) (schema.TableMeta, error)
	GetTableName() string
}

func BuildUndoItem(
	isBinary bool,
	sqlType constant.SQLType,
	schemaName, tableName,
	lockKey string, beforeImage,
	afterImage *schema.TableRecords) *undolog.SqlUndoLog {
	sqlUndoLog := &undolog.SqlUndoLog{
		IsBinary:    isBinary,
		SqlType:     sqlType,
		SchemaName:  schemaName,
		TableName:   tableName,
		LockKey:     lockKey,
		BeforeImage: beforeImage,
		AfterImage:  afterImage,
	}
	return sqlUndoLog
}
