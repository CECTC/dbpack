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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCollectRowKeys(t *testing.T) {
	cases := map[string]struct {
		lockKey    string
		resourceID string
		expected   []string
	}{
		"simple-1": {
			lockKey:    "test1:test2,test3;test4:test5,test6",
			resourceID: "resourceID",
			expected:   []string{"resourceID^^^test1^^^test2", "resourceID^^^test1^^^test3", "resourceID^^^test4^^^test5", "resourceID^^^test4^^^test6"},
		},
		"simple-2": {
			lockKey:    "table1:test2,test3;table2:test5,test6",
			resourceID: "resourceID",
			expected:   []string{"resourceID^^^table1^^^test2", "resourceID^^^table1^^^test3", "resourceID^^^table2^^^test5", "resourceID^^^table2^^^test6"},
		},
		"simple-3": {
			lockKey:    "table1:mergedPKs1,mergedPKs2;table2:mergedPKs3,mergedPKs4",
			resourceID: "resourceID",
			expected:   []string{"resourceID^^^table1^^^mergedPKs1", "resourceID^^^table1^^^mergedPKs2", "resourceID^^^table2^^^mergedPKs3", "resourceID^^^table2^^^mergedPKs4"},
		},
	}

	for caseTitle, tc := range cases {
		t.Run(caseTitle, func(t *testing.T) {
			result := CollectRowKeys(tc.lockKey, tc.resourceID)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestGetRowKey(t *testing.T) {
	cases := map[string]struct {
		resourceID string
		tableName  string
		pk         string
		expected   string
	}{
		"simple-1": {
			resourceID: "resourceID",
			tableName:  "tableName",
			pk:         "pk",
			expected:   "resourceID^^^tableName^^^pk",
		},
		"simple-2": {
			resourceID: "12345",
			tableName:  "nameOfTable",
			pk:         "PK",
			expected:   "12345^^^nameOfTable^^^PK",
		},
		"simple-3": {
			resourceID: "12345",
			tableName:  "nameOfTable",
			pk:         "",
			expected:   "12345^^^nameOfTable^^^",
		},
		"simple-4": {
			resourceID: "",
			tableName:  "nameOfTable",
			pk:         "",
			expected:   "^^^nameOfTable^^^",
		},
		"simple-5": {
			resourceID: "",
			tableName:  "",
			pk:         "",
			expected:   "^^^^^^",
		},
	}

	for caseTitle, tc := range cases {
		t.Run(caseTitle, func(t *testing.T) {
			result := GetRowKey(tc.resourceID, tc.tableName, tc.pk)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestParseTable(t *testing.T) {
	cases := []struct {
		tableName     string
		cutSet        string
		expectedDB    string
		expectedTable string
	}{
		{
			"`employees`.`employee`",
			"`",
			"employees",
			"employee",
		},
		{
			"employees.employee",
			"`",
			"employees",
			"employee",
		},
		{
			"`employee`",
			"`",
			"",
			"employee",
		},
		{
			"employee",
			"`",
			"",
			"employee",
		},
	}
	for _, tc := range cases {
		t.Run(tc.tableName, func(t *testing.T) {
			db, table := ParseTable(tc.tableName, tc.cutSet)
			assert.Equal(t, tc.expectedDB, db)
			assert.Equal(t, tc.expectedTable, table)
		})
	}
}
