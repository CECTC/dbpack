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

package schema

type ColumnMeta struct {
	TableCat        string
	TableSchemeName string
	TableName       string
	ColumnName      string
	DataType        int32
	DataTypeName    string
	ColumnSize      int64
	DecimalDigits   int64
	NumPrecRadix    int64
	Nullable        int32
	Remarks         string
	ColumnDef       string
	SqlDataType     int32
	SqlDatetimeSub  int32
	CharOctetLength int64
	OrdinalPosition int64
	IsNullable      string
	IsAutoIncrement string
}
