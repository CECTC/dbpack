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
	"reflect"
	"testing"

	"github.com/cectc/dbpack/third_party/parser/opcode"
)

func TestIsFloat64Equal(t *testing.T) {
	cases := map[string]struct {
		a, b float64
		want bool
	}{
		"0.0 == 0.0": {0.0, 0.0, true},
		"0.0 != 1.0": {0.0, 1.0, false},
		"1.0 == 1.0": {1.0, 1.0, true},
		"1.0 != 0.0": {1.0, 0.0, false},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			got := IsFloat64Equal(c.a, c.b)
			if got != c.want {
				t.Errorf("IsFloat64Equal(%v, %v) = %v, want %v", c.a, c.b, got, c.want)
			}
		})
	}
}

func TestIsFloat32Equal(t *testing.T) {
	cases := map[string]struct {
		a, b float32
		want bool
	}{
		"0.0 == 0.0": {0.0, 0.0, true},
		"0.0 != 1.0": {0.0, 1.0, false},
		"1.0 == 1.0": {1.0, 1.0, true},
		"1.0 != 0.0": {1.0, 0.0, false},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			got := IsFloat32Equal(c.a, c.b)
			if got != c.want {
				t.Errorf("IsFloat32Equal(%v, %v) = %v, want %v", c.a, c.b, got, c.want)
			}
		})
	}
}

func TestIsZero(t *testing.T) {
	cases := map[string]struct {
		a    interface{}
		want bool
	}{
		"int":    {0, true},
		"float":  {0.0, true},
		"string": {"", true},
		"struct": {struct{}{}, true},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			got := IsZero(c.a)
			if got != c.want {
				t.Errorf("IsZero(%v) = %v,want %v", c.a, got, c.want)
			}
		})
	}
}

func TestSortInt32s(t *testing.T) {
	cases := map[string]struct {
		a    []int32
		want []int32
	}{
		"empty": {[]int32{}, []int32{}},
		"one":   {[]int32{1}, []int32{1}},
		"two":   {[]int32{2, 1}, []int32{1, 2}},
		"three": {[]int32{3, 2, 1}, []int32{1, 2, 3}},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			SortInt32s(c.a)
			if !reflect.DeepEqual(c.a, c.want) {
				t.Errorf("SortInt32s(%v) = %v,want %v", c.a, c.a, c.want)
			}
		})
	}
}

func TestCompare(t *testing.T) {
	cases := map[string]struct {
		a, b interface{}
		want int
	}{
		"int":    {1, 2, -1},
		"float":  {1.0, 2.0, -1},
		"string": {"a", "b", -1},
		"slice":  {[]int{1, 2, 3}, []int{1, 2, 3}, 0},
		"map":    {map[string]string{"a": "a", "b": "b"}, map[string]string{"a": "a", "b": "b"}, 0},
		"struct": {struct{ a int }{1}, "1", 1},
		"chan":   {make(chan int), make(chan int), -1},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			got := Compare(c.a, c.b)
			if got != c.want {
				t.Errorf("Compare(%v,%v) = %v,want %v", c.a, c.b, got, c.want)
			}
		})
	}
}

func TestComputeUnary(t *testing.T) {
	cases := map[string]struct {
		op          opcode.Op
		a           interface{}
		want        interface{}
		expectedErr error
	}{
		"int":    {opcode.Not, 1, -1, nil},
		"float":  {opcode.BitNeg, 1.0, -1.0, nil},
		"string": {opcode.Not, "1", "-1", nil},
		"slice":  {opcode.Not2, []int{1, 2, 3}, []int{-1, -2, -3}, nil},
		"map":    {opcode.BitNeg, map[string]string{"a": "1", "b": "2"}, map[string]string{"a": "-1", "b": "-2"}, nil},
		"chan":   {opcode.Minus, make(chan int), nil, nil},
		"func":   {opcode.Plus, func() int { return 1 }, nil, nil},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := ComputeUnary(c.op, c.a)
			if !reflect.DeepEqual(err, c.expectedErr) {
				t.Errorf("ComputeUnary(%v,%v) = %v,%v,want %v,%v", c.op, c.a, got, err, c.want, c.expectedErr)
			}
		})
	}
}
