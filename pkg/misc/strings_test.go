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

import "testing"

func TestFirstNonEmptyString(t *testing.T) {
	cases := map[string]struct {
		first    string
		second   string
		others   []string
		expected string
	}{
		"empty": {
			first:    "",
			second:   "",
			others:   []string{},
			expected: "",
		},
		"a": {
			first:    "a",
			second:   "",
			others:   []string{},
			expected: "a",
		},
		"b": {
			first:    "",
			second:   "b",
			others:   []string{},
			expected: "b",
		},
		"hello": {
			first:    "hello",
			second:   "",
			others:   []string{},
			expected: "hello",
		},
		"world": {
			first:    "world",
			second:   "",
			others:   []string{},
			expected: "world",
		},
		"three": {
			first:    "",
			second:   "",
			others:   []string{"three"},
			expected: "three",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			actual := FirstNonEmptyString(tc.first, tc.second, tc.others...)
			if actual != tc.expected {
				t.Errorf("expected %s, got %s", tc.expected, actual)
			}
		})
	}
}

func TestMustFirstNonEmptyString(t *testing.T) {
	cases := map[string]struct {
		first    string
		second   string
		others   []string
		expected string
	}{
		"success-1": {
			first:    "a",
			second:   "b",
			others:   []string{},
			expected: "a",
		},
		"success-2": {
			first:    "a",
			second:   "b",
			others:   []string{"c", "d"},
			expected: "a",
		},
		"success-3": {
			first:    "hello",
			second:   "",
			others:   []string{},
			expected: "hello",
		},
		"success-4": {
			first:    "hello",
			second:   "world",
			others:   []string{},
			expected: "hello",
		},
		"success-5": {
			first:    "",
			second:   "",
			others:   []string{"three"},
			expected: "three",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			actual := MustFirstNonEmptyString(tc.first, tc.second, tc.others...)
			if actual != tc.expected {
				t.Errorf("expected %s, got %s", tc.expected, actual)
			}
		})
	}
}

func TestFirstNonZeroInt(t *testing.T) {
	cases := map[string]struct {
		first    int
		second   int
		others   []int
		expected int
	}{
		"success-1": {
			first:    0,
			second:   0,
			others:   []int{},
			expected: 0,
		},
		"success-2": {
			first:    1,
			second:   0,
			others:   []int{},
			expected: 1,
		},
		"success-3": {
			first:    0,
			second:   1,
			others:   []int{},
			expected: 1,
		},
		"success-4": {
			first:    1,
			second:   0,
			others:   []int{},
			expected: 1,
		},
		"success-5": {
			first:    0,
			second:   1,
			others:   []int{},
			expected: 1,
		},
		"success-6": {
			first:    0,
			second:   0,
			others:   []int{3},
			expected: 3,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			actual := FirstNonZeroInt(tc.first, tc.second, tc.others...)
			if actual != tc.expected {
				t.Errorf("expected %d, got %d", tc.expected, actual)
			}
		})
	}
}

func TestFirstNonZeroInt32(t *testing.T) {
	cases := map[string]struct {
		first    int32
		second   int32
		others   []int32
		expected int32
	}{
		"success-1": {
			first:    0,
			second:   0,
			others:   []int32{},
			expected: 0,
		},
		"success-2": {
			first:    1,
			second:   0,
			others:   []int32{},
			expected: 1,
		},
		"success-3": {
			first:    0,
			second:   1,
			others:   []int32{},
			expected: 1,
		},
		"success-4": {
			first:    1,
			second:   0,
			others:   []int32{},
			expected: 1,
		},
		"success-5": {
			first:    0,
			second:   1,
			others:   []int32{},
			expected: 1,
		},
		"success-6": {
			first:    0,
			second:   0,
			others:   []int32{3},
			expected: 3,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			actual := FirstNonZeroInt32(tc.first, tc.second, tc.others...)
			if actual != tc.expected {
				t.Errorf("expected %d, got %d", tc.expected, actual)
			}
		})
	}
}

func TestFirstNonZeroInt64(t *testing.T) {
	cases := map[string]struct {
		first    int64
		second   int64
		others   []int64
		expected int64
	}{
		"success-1": {
			first:    0,
			second:   0,
			others:   []int64{},
			expected: 0,
		},
		"success-2": {
			first:    1,
			second:   0,
			others:   []int64{},
			expected: 1,
		},
		"success-3": {
			first:    0,
			second:   1,
			others:   []int64{},
			expected: 1,
		},
		"success-4": {
			first:    1,
			second:   0,
			others:   []int64{},
			expected: 1,
		},
		"success-5": {
			first:    0,
			second:   1,
			others:   []int64{},
			expected: 1,
		},
		"success-6": {
			first:    0,
			second:   0,
			others:   []int64{3},
			expected: 3,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			actual := FirstNonZeroInt64(tc.first, tc.second, tc.others...)
			if actual != tc.expected {
				t.Errorf("expected %d,got %d", tc.expected, actual)
			}
		})
	}
}

func TestPadLeft(t *testing.T) {
	cases := map[string]struct {
		input    string
		pad      string
		length   int
		expected string
	}{
		"success-1": {
			input:    "test",
			pad:      "0",
			length:   10,
			expected: "000000test",
		},
		"success-2": {
			input:    "test",
			pad:      "0",
			length:   5,
			expected: "0test",
		},
		"success-3": {
			input:    "test",
			pad:      "0",
			length:   3,
			expected: "tes",
		},
		"success-4": {
			input:    "test",
			pad:      "0",
			length:   1,
			expected: "t",
		},
		"success-5": {
			input:    "test",
			pad:      "0",
			length:   0,
			expected: "",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			actual := PadLeft(tc.input, tc.pad, tc.length)
			if actual != tc.expected {
				t.Errorf("expected %s, got %s", tc.expected, actual)
			}
		})
	}
}

func TestPadRight(t *testing.T) {
	cases := map[string]struct {
		input    string
		pad      string
		length   int
		expected string
	}{
		"success-1": {
			input:    "test",
			pad:      "0",
			length:   10,
			expected: "test000000",
		},
		"success-2": {
			input:    "test",
			pad:      "0",
			length:   5,
			expected: "test0",
		},
		"success-3": {
			input:    "test",
			pad:      "0",
			length:   3,
			expected: "tes",
		},
		"success-4": {
			input:    "test",
			pad:      "0",
			length:   1,
			expected: "t",
		},
		"success-5": {
			input:    "test",
			pad:      "0",
			length:   0,
			expected: "",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			actual := PadRight(tc.input, tc.pad, tc.length)
			if actual != tc.expected {
				t.Errorf("expected %s, got %s", tc.expected, actual)
			}
		})
	}
}

func TestIsBlank(t *testing.T) {
	cases := map[string]struct {
		input    string
		expected bool
	}{
		"success-1": {
			input:    "",
			expected: true,
		},
		"success-2": {
			input:    " ",
			expected: true,
		},
		"success-3": {
			input:    "\t",
			expected: true,
		},
		"success-4": {
			input:    "\n",
			expected: true,
		},
		"success-5": {
			input:    "\r",
			expected: true,
		},
		"success-6": {
			input:    "\r\n",
			expected: true,
		},
		"success-7": {
			input:    "\n\r",
			expected: true,
		},
		"success-8": {
			input:    "\r\n\r\n",
			expected: true,
		},
		"success-9": {
			input:    "test",
			expected: false,
		},
		"success-10": {
			input:    "test\n",
			expected: false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			actual := IsBlank(tc.input)
			if actual != tc.expected {
				t.Errorf("expected %t, got %t", tc.expected, actual)
			}
		})
	}
}
