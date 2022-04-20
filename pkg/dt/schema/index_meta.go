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

type IndexType byte

const (
	IndexTypePrimary IndexType = iota
	IndexTypeNormal
	IndexTypeUnique
	IndexTypeFullText
)

type IndexMeta struct {
	Values          []ColumnMeta
	NonUnique       bool
	IndexQualifier  string
	IndexName       string
	ColumnName      string
	Type            int16
	IndexType       IndexType
	AscOrDesc       string
	Cardinality     int32
	OrdinalPosition int32
}
