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
	"strings"

	"github.com/cectc/dbpack/pkg/driver"
	"github.com/cectc/dbpack/pkg/dt/schema"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/meta"
	"github.com/cectc/dbpack/pkg/resource"
	"github.com/cectc/dbpack/third_party/parser/ast"
	"github.com/cectc/dbpack/third_party/parser/format"
)

type queryUpdateExecutor struct {
	conn        *driver.BackendConnection
	stmt        *ast.UpdateStmt
	beforeImage *schema.TableRecords
}

func NewQueryUpdateExecutor(
	conn *driver.BackendConnection,
	stmt *ast.UpdateStmt,
	beforeImage *schema.TableRecords) Executor {
	return &queryUpdateExecutor{
		conn:        conn,
		stmt:        stmt,
		beforeImage: beforeImage,
	}
}

func (executor *queryUpdateExecutor) BeforeImage(ctx context.Context) (*schema.TableRecords, error) {
	// todo
	return nil, nil
}

func (executor *queryUpdateExecutor) AfterImage(ctx context.Context) (*schema.TableRecords, error) {
	// todo
	return nil, nil
}

func (executor *queryUpdateExecutor) GetTableMeta(ctx context.Context) (schema.TableMeta, error) {
	dbName := executor.conn.DataSourceName()
	db := resource.GetDBManager().GetDB(dbName)
	return meta.GetTableMetaCache().GetTableMeta(ctx, db, executor.GetTableName())
}

func (executor *queryUpdateExecutor) GetTableName() string {
	var sb strings.Builder
	if err := executor.stmt.TableRefs.TableRefs.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
		log.Panic(err)
	}
	return sb.String()
}
