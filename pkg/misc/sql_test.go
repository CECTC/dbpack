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

func TestMysqlAppendInParam(t *testing.T) {
	cases := map[string]struct {
		in  int
		out string
	}{
		"1":  {1, "(?)"},
		"2":  {2, "(?,?)"},
		"3":  {3, "(?,?,?)"},
		"4":  {4, "(?,?,?,?)"},
		"5":  {5, "(?,?,?,?,?)"},
		"6":  {6, "(?,?,?,?,?,?)"},
		"7":  {7, "(?,?,?,?,?,?,?)"},
		"8":  {8, "(?,?,?,?,?,?,?,?)"},
		"9":  {9, "(?,?,?,?,?,?,?,?,?)"},
		"10": {10, "(?,?,?,?,?,?,?,?,?,?)"},
	}

	for caseTitle, tc := range cases {
		t.Run(caseTitle, func(t *testing.T) {
			result := MysqlAppendInParam(tc.in)
			assert.Equal(t, tc.out, result)
		})
	}
}

func TestPgsqlAppendInParam(t *testing.T) {
	cases := map[string]struct {
		in  int
		out string
	}{
		"1":  {1, "($1)"},
		"2":  {2, "($1,$2)"},
		"3":  {3, "($1,$2,$3)"},
		"4":  {4, "($1,$2,$3,$4)"},
		"5":  {5, "($1,$2,$3,$4,$5)"},
		"6":  {6, "($1,$2,$3,$4,$5,$6)"},
		"7":  {7, "($1,$2,$3,$4,$5,$6,$7)"},
		"8":  {8, "($1,$2,$3,$4,$5,$6,$7,$8)"},
		"9":  {9, "($1,$2,$3,$4,$5,$6,$7,$8,$9)"},
		"10": {10, "($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)"},
	}

	for caseTitle, tc := range cases {
		t.Run(caseTitle, func(t *testing.T) {
			result := PgsqlAppendInParam(tc.in)
			assert.Equal(t, tc.out, result)
		})
	}
}

func TestMysqlAppendInParamWithValue(t *testing.T) {
	cases := map[string]struct {
		in  []interface{}
		out string
	}{
		"1": {
			in:  []interface{}{"abc", "xyz"},
			out: "('abc','xyz')",
		},
		"2": {
			in:  []interface{}{[]byte("abc"), []byte("xyz")},
			out: "('abc','xyz')",
		},
		"3": {
			in:  []interface{}{1, 2, 3},
			out: "(1,2,3)",
		},
		"4": {
			in:  []interface{}{int64(1), int64(2), int64(3)},
			out: "(1,2,3)",
		},
	}
	for caseTitle, tc := range cases {
		t.Run(caseTitle, func(t *testing.T) {
			result := MysqlAppendInParamWithValue(tc.in)
			assert.Equal(t, tc.out, result)
		})
	}
}
