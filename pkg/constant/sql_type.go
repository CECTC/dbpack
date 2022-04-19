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

package constant

import "fmt"

type SQLType byte

const (
	SQLType_SELECT SQLType = iota

	SQLType_INSERT

	SQLType_UPDATE

	SQLType_DELETE

	SQLType_SELECT_FOR_UPDATE

	SQLType_REPLACE

	SQLType_TRUNCATE

	SQLType_CREATE

	SQLType_DROP

	SQLType_LOAD

	SQLType_MERGE

	SQLType_SHOW

	SQLType_ALTER

	SQLType_RENAME

	SQLType_DUMP

	SQLType_DEBUG

	SQLType_EXPLAIN

	SQLType_PROCEDURE

	SQLType_DESC

	// ******************************************
	// 一些 java mybatis 特有的 sql 方法省略
	// ******************************************

	SQLType_SET SQLType = 27

	SQLType_RELOAD SQLType = 28

	SQLType_SELECT_UNION SQLType = 29

	SQLType_CREATE_TABLE SQLType = 30

	SQLType_DROP_TABLE SQLType = 31

	SQLType_ALTER_TABLE SQLType = 32

	SQLType_SAVE_POINT SQLType = 33

	SQLType_SELECT_FROM_UPDATE SQLType = 34

	SQLType_MULTI_DELETE SQLType = 35

	SQLType_MULTI_UPDATE SQLType = 36

	SQLType_CREATE_INDEX SQLType = 37

	SQLType_DROP_INDEX SQLType = 38

	// ******************************************
	// 一些不常见的 sql 类型省略，确有需要，以后再加
	// ******************************************
)

func (sqlType SQLType) String() string {
	switch sqlType {
	case SQLType_SELECT:
		return "SELECT"

	case SQLType_INSERT:
		return "INSERT"

	case SQLType_UPDATE:
		return "UPDATE"

	case SQLType_DELETE:
		return "DELETE"

	case SQLType_SELECT_FOR_UPDATE:
		return "SELECT_FOR_UPDATE"

	case SQLType_REPLACE:
		return "REPLACE"

	case SQLType_TRUNCATE:
		return "TRUNCATE"

	case SQLType_CREATE:
		return "CREATE"

	case SQLType_DROP:
		return "DROP"

	case SQLType_LOAD:
		return "LOAD"

	case SQLType_MERGE:
		return "MERGE"

	case SQLType_SHOW:
		return "SHOW"

	case SQLType_ALTER:
		return "ALTER"

	case SQLType_RENAME:
		return "RENAME"

	case SQLType_DUMP:
		return "DUMP"

	case SQLType_DEBUG:
		return "DEBUG"

	case SQLType_EXPLAIN:
		return "EXPLAIN"

	case SQLType_PROCEDURE:
		return "PROCEDURE"

	case SQLType_DESC:
		return "DESC"

	case SQLType_SET:
		return "SET"

	case SQLType_RELOAD:
		return "RELOAD"

	case SQLType_SELECT_UNION:
		return "SELECT_UNION"

	case SQLType_CREATE_TABLE:
		return "CREATE_TABLE"

	case SQLType_DROP_TABLE:
		return "DROP_TABLE"

	case SQLType_ALTER_TABLE:
		return "ALTER_TABLE"

	case SQLType_SAVE_POINT:
		return "SAVE_POINT"

	case SQLType_SELECT_FROM_UPDATE:
		return "SELECT_FROM_UPDATE"

	case SQLType_MULTI_DELETE:
		return "MULTI_DELETE"

	case SQLType_MULTI_UPDATE:
		return "MULTI_UPDATE"

	case SQLType_CREATE_INDEX:
		return "CREATE_INDEX"

	case SQLType_DROP_INDEX:
		return "DROP_INDEX"
	default:
		return fmt.Sprintf("%d", sqlType)
	}
}
