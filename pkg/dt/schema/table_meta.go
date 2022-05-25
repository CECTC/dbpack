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

type TableMeta struct {
	SchemaName string
	TableName  string
	Columns    []string
	AllColumns map[string]ColumnMeta
	AllIndexes map[string]IndexMeta
}

func (meta TableMeta) GetPrimaryKeyMap() map[string]ColumnMeta {
	pk := make(map[string]ColumnMeta)
	for _, index := range meta.AllIndexes {
		if index.IndexType == IndexTypePrimary {
			for _, col := range index.Values {
				pk[col.ColumnName] = col
			}
		}
	}
	if len(pk) > 1 {
		log.Panicf("%s contains multi PK, but current not support.", meta.TableName)
	}
	if len(pk) < 1 {
		log.Panicf("%s needs to contain the primary key.", meta.TableName)
	}
	return pk
}

func (meta TableMeta) GetPrimaryKeyOnlyName() []string {
	list := make([]string, 0)
	pk := meta.GetPrimaryKeyMap()
	for key := range pk {
		list = append(list, key)
	}
	return list
}

func (meta TableMeta) GetPKName() string {
	return meta.GetPrimaryKeyOnlyName()[0]
}
