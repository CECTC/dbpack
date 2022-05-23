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

func TestFormatTimeMillis(t *testing.T) {
	cases := map[string]struct {
		in  uint64
		out string
	}{
		"1": {4, "1970-01-01 00:00:00"},
		"2": {1844674, "1970-01-01 00:30:44"},
		"3": {18446744073709551615, "1969-12-31 23:59:59"},
		"4": {1653055405, "1970-01-20 03:10:55"},
		"5": {45408320589235234, "1730-04-30 13:42:42"},
		"6": {123456, "1970-01-01 00:02:03"},
	}

	for caseTitle, tc := range cases {
		t.Run(caseTitle, func(t *testing.T) {
			result := FormatTimeMillis(tc.in)
			assert.Equal(t, tc.out, result)
		})
	}
}

func TestFormatDate(t *testing.T) {
	cases := map[string]struct {
		in  uint64
		out string
	}{
		"1": {4, "1970-01-01"},
		"2": {1844674, "1970-01-01"},
		"3": {18446744073709551615, "1969-12-31"},
		"4": {1653055405, "1970-01-20"},
		"5": {45408320589235234, "1730-04-30"},
		"6": {123456, "1970-01-01"},
	}

	for caseTitle, tc := range cases {
		t.Run(caseTitle, func(t *testing.T) {
			result := FormatDate(tc.in)
			assert.Equal(t, tc.out, result)
		})
	}
}
