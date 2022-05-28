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

package undolog

import (
	"github.com/cectc/dbpack/pkg/constant"
	"github.com/cectc/dbpack/pkg/dt/schema"
)

type SqlUndoLog struct {
	// IsBinary binary protocol or text protocol, com_stmt_execute corresponds to
	// binary protocol (prepared statement), com_query corresponds to text protocol
	// (text statement).
	IsBinary bool
	// SqlType insert、delete、update
	SqlType     constant.SQLType
	SchemaName  string
	TableName   string
	LockKey     string
	BeforeImage *schema.TableRecords
	AfterImage  *schema.TableRecords
}

func (undoLog *SqlUndoLog) SetTableMeta(tableMeta schema.TableMeta) {
	if undoLog.BeforeImage != nil {
		undoLog.BeforeImage.TableMeta = tableMeta
	}
	if undoLog.AfterImage != nil {
		undoLog.AfterImage.TableMeta = tableMeta
	}
}

func (undoLog *SqlUndoLog) GetUndoRows() *schema.TableRecords {
	if undoLog.SqlType == constant.SQLType_UPDATE ||
		undoLog.SqlType == constant.SQLType_DELETE {
		return undoLog.BeforeImage
	} else if undoLog.SqlType == constant.SQLType_INSERT {
		return undoLog.AfterImage
	}
	return nil
}

type BranchUndoLog struct {
	Xid         string
	BranchID    int64
	SqlUndoLogs []*SqlUndoLog
}
