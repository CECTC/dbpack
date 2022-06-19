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
	"github.com/cectc/dbpack/pkg/constant"
	"github.com/cectc/dbpack/third_party/parser/ast"
)

type (
	Value struct {
		Typ   constant.FieldType
		Flags uint
		Len   int
		Val   interface{}
		Raw   []byte
	}

	Field interface {
		FiledName() string

		TableName() string

		DataBaseName() string

		TypeDatabaseName() string
	}

	// Rows is an iterator over an executed query's results.
	Row interface {
		// Columns returns the names of the columns. The number of
		// columns of the result is inferred from the length of the
		// slice. If a particular column name isn't known, an empty
		// string should be returned for that entry.
		Columns() []string

		Fields() []Field

		// Data
		Data() []byte

		Decode() ([]*Value, error)
	}

	// Result is the result of a query execution.
	Result interface {
		// LastInsertId returns the database's auto-generated ID
		// after, for example, an INSERT into a table with primary
		// key.
		LastInsertId() (uint64, error)

		// RowsAffected returns the number of rows affected by the
		// query.
		RowsAffected() (uint64, error)
	}

	// Stmt is a buffer used for store prepare statement meta data
	Stmt struct {
		StatementID      uint32
		HasLongDataParam bool
		SqlText          string
		ParamsCount      uint16
		ParamData        []byte
		ParamsType       []int32
		ColumnNames      []string
		BindVars         map[string]interface{}
		StmtNode         ast.StmtNode
	}
)
