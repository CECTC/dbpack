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
	"strings"
	"unicode/utf8"
)

const (
	_ EscapeFlag = iota
	EscapeSingleQuote
	EscapeDoubleQuote
	EscapeLike
)

type EscapeFlag uint8

func WriteEscape(sb *strings.Builder, input string, flag EscapeFlag) {
	for len(input) > 0 {
		s, size := utf8.DecodeRuneInString(input)
		switch s {
		case '\n':
			sb.WriteString(`\n`)
		case '\b':
			sb.WriteString(`\b`)
		case '\t':
			sb.WriteString(`\t`)
		case '\r':
			sb.WriteString(`\r`)
		case '\\':
			// process LIKE literal, keep '\%' and '\_'
			var isLikeEscape bool
			if flag&EscapeLike != 0 {
				next, _ := utf8.DecodeRuneInString(input[size:])
				isLikeEscape = next == '%' || next == '_'
			}
			sb.WriteByte('\\')
			if !isLikeEscape {
				sb.WriteByte('\\')
			}
		case '\'':
			if flag&EscapeSingleQuote != 0 {
				sb.WriteByte('\\')
			}
			sb.WriteByte('\'')
		case '"':
			if flag&EscapeDoubleQuote != 0 {
				sb.WriteByte('\\')
			}
			sb.WriteByte('"')
		default:
			sb.WriteRune(s)
		}

		input = input[size:]
	}
}

func Escape(input string, flag EscapeFlag) string {
	var sb strings.Builder
	sb.Grow(len(input) + 8)

	WriteEscape(&sb, input, flag)

	return sb.String()
}

func Unescape(s string, ignores ...rune) string {
	if !strings.ContainsRune(s, '\\') {
		return s
	}

	var sb strings.Builder
	sb.Grow(len(s))

	var escape bool
	for _, r := range s {
		if r == '\\' {
			if escape {
				sb.WriteByte('\\')
			}
			escape = !escape
			continue
		}

		if !escape {
			sb.WriteRune(r)
			continue
		}
		escape = false

		switch r {
		case 'b':
			sb.WriteByte('\b')
		case 'n':
			sb.WriteByte('\n')
		case 't':
			sb.WriteByte('\t')
		case 'r':
			sb.WriteByte('\r')
		case 'v':
			sb.WriteByte('\v')
		case '"':
			sb.WriteByte('"')
		case '\'':
			sb.WriteByte('\'')
		default:
			var ignore bool
			for _, it := range ignores {
				if it == r {
					ignore = true
					break
				}
			}

			if ignore {
				sb.WriteByte('\\')
			}

			sb.WriteRune(r)
		}
	}

	return sb.String()
}
