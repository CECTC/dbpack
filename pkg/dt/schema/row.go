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

import (
	"github.com/cectc/dbpack/pkg/log"
)

type Row struct {
	Fields []*Field
}

func (row *Row) PrimaryKeys() []*Field {
	fields := make([]*Field, 0)
	for _, field := range row.Fields {
		if field.KeyType == PrimaryKey {
			fields = append(fields, field)
		}
	}
	if len(fields) > 1 {
		log.Panicf("Multi-PK")
	}
	return fields
}

func (row *Row) NonPrimaryKeys() []*Field {
	fields := make([]*Field, 0)
	for _, field := range row.Fields {
		if field.KeyType != PrimaryKey {
			fields = append(fields, field)
		}
	}
	return fields
}
