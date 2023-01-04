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

package misc

import (
	"fmt"
	"strings"
)

func CollectRowKeys(lockKey, resourceID string) []string {
	var locks = make([]string, 0)
	tableGroupedLockKeys := strings.Split(lockKey, ";")
	for _, tableGroupedLockKey := range tableGroupedLockKeys {
		if tableGroupedLockKey != "" {
			idx := strings.Index(tableGroupedLockKey, ":")
			if idx < 0 {
				return nil
			}

			tableName := tableGroupedLockKey[0:idx]
			mergedPKs := tableGroupedLockKey[idx+1:]

			if mergedPKs == "" {
				return nil
			}

			pks := strings.Split(mergedPKs, ",")
			if len(pks) == 0 {
				return nil
			}

			for _, pk := range pks {
				if pk != "" {
					locks = append(locks, GetRowKey(resourceID, tableName, pk))
				}
			}
		}
	}
	return locks
}

func GetRowKey(resourceID string, tableName string, pk string) string {
	return fmt.Sprintf("%s^^^%s^^^%s", resourceID, tableName, pk)
}

// ParseTable return db, table name. If db is empty, return "".
func ParseTable(tableName, cutSet string) (string, string) {
	if strings.Contains(tableName, ".") {
		idx := strings.LastIndex(tableName, ".")
		db := tableName[0:idx]
		tName := tableName[idx+1:]
		return strings.Trim(db, cutSet), strings.Trim(tName, cutSet)
	} else {
		return "", strings.Trim(tableName, cutSet)
	}
}

func ParseColumn(columnName string) string {
	if strings.Contains(columnName, ".") {
		idx := strings.LastIndex(columnName, ".")
		return columnName[idx+1:]
	}
	return columnName
}
