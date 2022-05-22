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

package function

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStringFunc(t *testing.T) {
	cases := []struct {
		f      string
		expect interface{}
		args   []interface{}
	}{
		{"LENGTH", int64(14), []interface{}{"hello，Mr.王"}},
		{"CHAR_LENGTH", int64(10), []interface{}{"hello，Mr.王"}},
		{"UPPER", "ABC", []interface{}{"abc"}},
		{"LOWER", "abc", []interface{}{"ABC"}},
		{"LEFT", "a", []interface{}{"abc", 1}},
		{"RIGHT", "bc", []interface{}{"abc", 2}},
		{"LTRIM", "abc  ", []interface{}{"  abc  "}},
		{"RTRIM", "  abc", []interface{}{"  abc  "}},
		{"LPAD", "aaab", []interface{}{"b", 4, "a"}},
		{"RPAD", "baaa", []interface{}{"b", 4, "a"}},
		{"SUBSTRING", "abc", []interface{}{"abcdef", 1, 3}},
		{"SUBSTRING", "", []interface{}{"abcdef", 0, 3}},
		{"SUBSTRING", "", []interface{}{"abcdef", 1, 0}},
		{"SUBSTRING", "def", []interface{}{"abcdef", -3, 3}},
		{"REVERSE", "fedcba", []interface{}{"abcdef"}},
		{"REPEAT", "ababab", []interface{}{"ab", 3}},
		{"CONCAT", "abcd", []interface{}{"ab", "cd"}},
		{"CONCAT_WS", "ab", []interface{}{",", "ab"}},
		{"CONCAT_WS", "ab,cd", []interface{}{",", "ab", "cd"}},
		{"SPACE", "   ", []interface{}{3}},
		{"REPLACE", "xyzdef", []interface{}{"abcdef", "abc", "xyz"}},
		{"STRCMP", int64(-1), []interface{}{"abc", "def"}},
		{"STRCMP", int64(1), []interface{}{"def", "abc"}},
	}
	for _, c := range cases {
		t.Run(c.f, func(t *testing.T) {
			v, err := eval(c.f, c.args...)
			if assert.NoError(t, err, c.f) {
				assert.Equal(t, c.expect, v)
			}
		})
	}
}

func TestTimeFunc(t *testing.T) {
	for _, c := range []string{
		"NOW", "CURDATE", "CURRENT_DATE", "SYSDATE", "UNIX_TIMESTAMP", "CURTIME",
	} {
		t.Run(c, func(t *testing.T) {
			v, err := eval(c)
			assert.NoError(t, err)
			t.Logf("%s: %v\n", c, v)
		})
	}

	now := time.Now()

	v, err := eval("MONTHNAME", now)
	assert.NoError(t, err)
	t.Log("MONTHNAME:", v)

	v, err = eval("MONTH", now)
	assert.NoError(t, err)
	assert.Equal(t, int64(now.Month()), v)

	v, err = eval("DAY", now)
	assert.NoError(t, err)
	assert.Equal(t, int64(now.Day()), v)

	v, err = eval("ADDDATE", now, 1)
	assert.NoError(t, err)
	t.Log("ADDDATE:", v)

	v, err = eval("SUBDATE", now, 1)
	assert.NoError(t, err)
	t.Log("SUBDATE:", v)

	v, err = eval("FROM_UNIXTIME", now.Unix()+24*3600)
	assert.NoError(t, err)
	t.Log("FROM_UNIXTIME:", v)

	v, err = eval("DATE_FORMAT", now, "%Y-%m-%d")
	assert.NoError(t, err)
	t.Log("DATE_FORMAT:", v)
}

func TestNumberFunc(t *testing.T) {
	cases := []struct {
		f      string
		expect interface{}
		args   []interface{}
	}{
		{"ABS", int64(1), []interface{}{-1}},
		{"CEIL", int64(4), []interface{}{3.14}},
		{"FLOOR", int64(3), []interface{}{3.14}},
		{"ROUND", int64(3), []interface{}{3.14}},
		{"POW", int64(4), []interface{}{2, 2}},
		{"POWER", int64(4), []interface{}{2, 2}},
		{"SQRT", int64(2), []interface{}{4}},
		{"MOD", int64(5), []interface{}{5, 10}},
	}
	for _, c := range cases {
		t.Run(c.f, func(t *testing.T) {
			v, err := eval(c.f, c.args...)
			if assert.NoError(t, err, c.f) {
				assert.Equal(t, c.expect, v)
			}
		})
	}

	for _, c := range []string{"RAND", "PI"} {
		t.Run(c, func(t *testing.T) {
			v, err := eval(c)
			assert.NoError(t, err)
			t.Logf("%s: %v\n", c, v)
		})
	}
}

func TestOtherFunc(t *testing.T) {
	cases := []struct {
		f      string
		expect interface{}
		args   []interface{}
	}{
		{"IF", "a", []interface{}{true, "a", "b"}},
		{"IF", "b", []interface{}{false, "a", "b"}},
		{"IFNULL", "a", []interface{}{nil, "a"}},
		{"IFNULL", "a", []interface{}{"a", "b"}},
		{"SHA", "b6589fc6ab0dc82cf12099d1c2d40ab994e8410c", []interface{}{0}},
		{"SHA", "b6589fc6ab0dc82cf12099d1c2d40ab994e8410c", []interface{}{"0"}},
		{"SHA", "a9993e364706816aba3e25717850c26c9cd0d89d", []interface{}{"abc"}},
		{"MD5", "900150983cd24fb0d6963f7d28e17f72", []interface{}{"abc"}},
		{"MD5", "9ef9f83efc764b649d5b80a975da48c9", []interface{}{time.Unix(0, 0)}},
	}
	for _, c := range cases {
		t.Run(c.f, func(t *testing.T) {
			v, err := eval(c.f, c.args...)
			if assert.NoError(t, err, c.f) {
				assert.Equal(t, c.expect, v)
			}
		})
	}
}

func eval(name string, args ...interface{}) (interface{}, error) {
	var sb strings.Builder
	sb.WriteString("$_")
	sb.WriteString(name)
	sb.WriteByte('(')
	if len(args) > 0 {
		sb.WriteString("arguments[0]")
		for i := 1; i < len(args); i++ {
			sb.WriteByte(',')
			sb.WriteString(fmt.Sprintf("arguments[%d]", i))
		}
	}
	sb.WriteByte(')')

	js := NewJSRuntime()
	return js.Eval(sb.String(), args)
}
