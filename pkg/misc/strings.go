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
	"regexp"
	"strings"
	"unicode"
)

const (
	Numeric = "^[0-9]+$"
)

var (
	rxNumeric = regexp.MustCompile(Numeric)
)

func FirstNonEmptyString(first string, second string, others ...string) string {
	if len(first) > 0 {
		return first
	}
	if len(second) > 0 {
		return second
	}

	for i := 0; i < len(others); i++ {
		if len(others[i]) > 0 {
			return others[i]
		}
	}
	return ""
}

func MustFirstNonEmptyString(first, second string, others ...string) string {
	v := FirstNonEmptyString(first, second, others...)
	if len(v) < 1 {
		panic("At least one non-empty string required!")
	}
	return v
}

func FirstNonZeroInt(first, second int, others ...int) int {
	if first != 0 {
		return first
	}
	if second != 0 {
		return second
	}

	for _, it := range others {
		if it != 0 {
			return it
		}
	}
	return 0
}

func FirstNonZeroInt32(first, second int32, others ...int32) int32 {
	if first != 0 {
		return first
	}
	if second != 0 {
		return second
	}

	for _, it := range others {
		if it != 0 {
			return it
		}
	}
	return 0
}

func FirstNonZeroInt64(first, second int64, others ...int64) int64 {
	if first != 0 {
		return first
	}
	if second != 0 {
		return second
	}

	for _, it := range others {
		if it != 0 {
			return it
		}
	}
	return 0
}

func PadLeft(str, pad string, length int) string {
	if len(str) == length {
		return str
	}

	if len(str) > length {
		return str[:length]
	}

	var (
		sb        strings.Builder
		prefixLen = length - len(str)
	)

	for {
		switch {
		case sb.Len() == prefixLen:
			sb.WriteString(str)
			return sb.String()
		case sb.Len() > prefixLen:
			return sb.String()[:prefixLen] + str
		default:
			sb.WriteString(pad)
		}
	}
}

func PadRight(str, pad string, length int) string {
	if len(str) >= length {
		return str[:length]
	}

	var sb strings.Builder
	sb.WriteString(str)

	for {
		switch {
		case sb.Len() == length:
			return sb.String()
		case sb.Len() > length:
			return sb.String()[:length]
		default:
			sb.WriteString(pad)
		}
	}
}

func IsBlank(s string) bool {
	if len(s) < 1 {
		return true
	}
	return strings.IndexFunc(s, func(r rune) bool {
		return !unicode.IsSpace(r)
	}) == -1
}

// IsNumeric checks if the string contains only numbers. Empty string is valid.
func IsNumeric(str string) bool {
	if len(str) == 0 {
		return true
	}
	return rxNumeric.MatchString(str)
}
