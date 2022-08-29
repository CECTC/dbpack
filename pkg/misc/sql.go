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

func MysqlAppendInParam(size int) string {
	var sb strings.Builder
	sb.WriteByte('(')
	for i := 0; i < size; i++ {
		sb.WriteByte('?')
		if i < size-1 {
			sb.WriteByte(',')
		}
	}
	sb.WriteByte(')')
	return sb.String()
}

func PgsqlAppendInParam(size int) string {
	var sb strings.Builder
	sb.WriteByte('(')
	for i := 0; i < size; i++ {
		sb.WriteString(fmt.Sprintf("$%d", i+1))
		if i < size-1 {
			sb.WriteByte(',')
		}
	}
	sb.WriteByte(')')
	return sb.String()
}

func MysqlAppendInParamWithValue(values []interface{}) string {
	var sb strings.Builder
	sb.WriteByte('(')
	for i, value := range values {
		switch val := value.(type) {
		case string:
			if strings.HasPrefix(val, "'") {
				sb.WriteString(fmt.Sprintf("%s", val))
			} else {
				sb.WriteString(fmt.Sprintf("'%s'", val))
			}
		case []byte:
			sb.WriteString(fmt.Sprintf("'%s'", val))
		default:
			sb.WriteString(fmt.Sprintf("%v", val))
		}
		if i < len(values)-1 {
			sb.WriteByte(',')
		}
	}
	sb.WriteByte(')')
	return sb.String()
}
