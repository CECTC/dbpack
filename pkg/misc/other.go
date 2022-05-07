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
	"math"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cectc/dbpack/third_party/parser/opcode"
)

const min = 0.000001

var (
	_numReg   = regexp.MustCompilePOSIX("^-?(0|[1-9][0-9]*)$")
	_floatReg = regexp.MustCompilePOSIX("^-?(0|[1-9][0-9]*)(.[0-9]+)?$")
)

func IsFloat64Equal(f1, f2 float64) bool {
	if f1 > f2 {
		return math.Dim(f1, f2) < min
	} else {
		return math.Dim(f2, f1) < min
	}
}

func IsFloat32Equal(f1, f2 float32) bool {
	return IsFloat64Equal(float64(f1), float64(f2))
}

func IsZero(value interface{}) bool {
	return value == reflect.Zero(reflect.TypeOf(value)).Interface()
}

func SortInt32s(s []int32) {
	sort.Sort(sortInt32s(s))
}

type sortInt32s []int32

func (s sortInt32s) Len() int {
	return len(s)
}

func (s sortInt32s) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s sortInt32s) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func Compare(a, b interface{}) int {
	s1, ok1 := a.(string)
	s2, ok2 := b.(string)

	if ok1 && ok2 {
		switch {
		case s1 > s2:
			return 1
		case s1 < s2:
			return -1
		default:
			return 0
		}
	}

	d1, ok1 := a.(time.Time)
	d2, ok2 := b.(time.Time)
	if ok1 && ok2 {
		if d1.Before(d2) {
			return -1
		}
		if d1.Equal(d2) {
			return 0
		}
		return 1
	}

	s1 = fmt.Sprintf("%v", a)
	s2 = fmt.Sprintf("%v", b)

	if _numReg.MatchString(s1) && _numReg.MatchString(s2) {
		n1, _ := strconv.ParseInt(s1, 10, 64)
		n2, _ := strconv.ParseInt(s2, 10, 64)
		switch {
		case n1 > n2:
			return 1
		case n1 < n2:
			return -1
		default:
			return 0
		}
	}

	if _floatReg.MatchString(s1) && _floatReg.MatchString(s2) {
		f1, _ := strconv.ParseFloat(s1, 64)
		f2, _ := strconv.ParseFloat(s1, 64)
		switch {
		case f1 > f2:
			return 1
		case f1 < f2:
			return -1
		default:
			return 0
		}

	}

	switch {
	case s1 > s2:
		return 1
	case s1 < s2:
		return -1
	default:
		return 0
	}
}

func ComputeUnary(op opcode.Op, input interface{}) (interface{}, error) {
	switch val := input.(type) {
	case string:
		f, _ := strconv.ParseFloat(val, 64)
		switch op {
		case opcode.Not, opcode.Not2:
			if f == 0 {
				return 1, nil
			}
			return 0, nil
		case opcode.Minus:
			return -f, nil
		case opcode.BitNeg:
			if f > 0 {
				return ^uint64(f), nil
			}
			return ^int64(f), nil
		}
	case bool:
		switch op {
		case opcode.Not, opcode.Not2:
			return !val, nil
		case opcode.Minus:
			if val {
				return -1, nil
			}
			return 0, nil
		case opcode.BitNeg:
			var n uint64
			if val {
				n = 1
			}
			return ^n, nil
		}
	case float64:
		if math.IsNaN(val) || math.IsInf(val, 0) {
			return val, nil
		}
		switch op {
		case opcode.Not, opcode.Not2:
			if val == 0 {
				return 1, nil
			}
			return 0, nil
		case opcode.Minus:
			return -val, nil
		case opcode.BitNeg:
			if val > 0 {
				return ^uint64(val), nil
			}
			return ^int64(val), nil
		}
	case float32:
		switch op {
		case opcode.Not, opcode.Not2:
			if val == 0 {
				return 1, nil
			}
			return 0, nil
		case opcode.Minus:
			return -val, nil
		case opcode.BitNeg:
			if val > 0 {
				return ^uint64(val), nil
			}
			return ^int64(val), nil
		}
	case int64:
		switch op {
		case opcode.Not, opcode.Not2:
			if val == 0 {
				return 1, nil
			}
			return 0, nil
		case opcode.Minus:
			return -val, nil
		case opcode.BitNeg:
			if val > 0 {
				return ^uint64(val), nil
			}
			return ^val, nil
		}
	case int:
		switch op {
		case opcode.Not, opcode.Not2:
			if val == 0 {
				return 1, nil
			}
			return 0, nil
		case opcode.Minus:
			return -val, nil
		case opcode.BitNeg:
			if val > 0 {
				return ^uint64(val), nil
			}
			return ^val, nil
		}
	case int32:
		switch op {
		case opcode.Not, opcode.Not2:
			if val == 0 {
				return 1, nil
			}
			return 0, nil
		case opcode.Minus:
			return -val, nil
		case opcode.BitNeg:
			if val > 0 {
				return ^uint64(val), nil
			}
			return ^int64(val), nil
		}
	case int16:
		switch op {
		case opcode.Not, opcode.Not2:
			if val == 0 {
				return 1, nil
			}
			return 0, nil
		case opcode.Minus:
			return -val, nil
		case opcode.BitNeg:
			if val > 0 {
				return ^uint64(val), nil
			}
			return ^int64(val), nil
		}
	case int8:
		switch op {
		case opcode.Not, opcode.Not2:
			if val == 0 {
				return 1, nil
			}
			return 0, nil
		case opcode.Minus:
			return -val, nil
		case opcode.BitNeg:
			if val > 0 {
				return ^uint64(val), nil
			}
			return ^int64(val), nil
		}
	case uint64:
		switch op {
		case opcode.Not, opcode.Not2:
			if val == 0 {
				return 1, nil
			}
			return 0, nil
		case opcode.Minus:
			return -int64(val), nil
		case opcode.BitNeg:
			return ^val, nil
		}
	case uint:
		switch op {
		case opcode.Not, opcode.Not2:
			if val == 0 {
				return uint(1), nil
			}
			return uint(0), nil
		case opcode.Minus:
			return -int(val), nil
		case opcode.BitNeg:
			return ^val, nil
		}
	case uint32:
		switch op {
		case opcode.Not, opcode.Not2:
			if val == 0 {
				return 1, nil
			}
			return 0, nil
		case opcode.Minus:
			return -int32(val), nil
		case opcode.BitNeg:
			return ^uint64(val), nil
		}
	case uint16:
		switch op {
		case opcode.Not, opcode.Not2:
			if val == 0 {
				return 1, nil
			}
			return 0, nil
		case opcode.Minus:
			return -int16(val), nil
		case opcode.BitNeg:
			return ^uint64(val), nil
		}
	case uint8:
		switch op {
		case opcode.Not, opcode.Not2:
			if val == 0 {
				return 1, nil
			}
			return 0, nil
		case opcode.Minus:
			return -int8(val), nil
		case opcode.BitNeg:
			return ^uint64(val), nil
		}
	}
	return input, nil
}

func Wrap(sb *strings.Builder, wrap byte, origin string) {
	sb.WriteByte(wrap)
	sb.WriteString(origin)
	sb.WriteByte(wrap)
}
