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
	"bytes"
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/cectc/dbpack/pkg/constant"
	"github.com/cectc/dbpack/pkg/errors"
)

var (
	sDate0           = "0000-00-00"
	sDateTime0       = "0000-00-00 00:00:00"
	someTime         = time.Unix(123, 0)
	answer     int64 = 42
)

func TestRegisterTLSConfig(t *testing.T) {
	err := RegisterTLSConfig("custom", &tls.Config{
		ServerName:         "localhost",
		InsecureSkipVerify: true,
	})
	assert.Equal(t, nil, err)

	cfg1 := GetTLSConfigClone("custom")
	assert.Equal(t, "localhost", cfg1.ServerName)
	assert.Equal(t, true, cfg1.InsecureSkipVerify)

	DeregisterTLSConfig("custom")
	cfg2 := GetTLSConfigClone("custom")
	assert.Nil(t, cfg2)
}

func TestReadBool(t *testing.T) {
	testCases := []struct {
		in          string
		expectValue bool
		expectValid bool
	}{
		{
			in:          "1",
			expectValue: true,
			expectValid: true,
		},
		{
			in:          "true",
			expectValue: true,
			expectValid: true,
		},
		{
			in:          "TRUE",
			expectValue: true,
			expectValid: true,
		},
		{
			in:          "True",
			expectValue: true,
			expectValid: true,
		},
		{
			in:          "0",
			expectValue: false,
			expectValid: true,
		},
		{
			in:          "false",
			expectValue: false,
			expectValid: true,
		},
		{
			in:          "FALSE",
			expectValue: false,
			expectValid: true,
		},
		{
			in:          "False",
			expectValue: false,
			expectValid: true,
		},
		{
			in:          "zero",
			expectValue: false,
			expectValid: false,
		},
	}

	for _, c := range testCases {
		t.Run(c.in, func(t *testing.T) {
			value, valid := ReadBool(c.in)
			assert.Equal(t, c.expectValue, value)
			assert.Equal(t, c.expectValid, valid)
		})
	}
}

func TestLengthEncodedInteger(t *testing.T) {
	var integerTests = []struct {
		num     uint64
		encoded []byte
	}{
		{0x0000000000000000, []byte{0x00}},
		{0x0000000000000012, []byte{0x12}},
		{0x00000000000000fa, []byte{0xfa}},
		{0x0000000000000100, []byte{0xfc, 0x00, 0x01}},
		{0x0000000000001234, []byte{0xfc, 0x34, 0x12}},
		{0x000000000000ffff, []byte{0xfc, 0xff, 0xff}},
		{0x0000000000010000, []byte{0xfd, 0x00, 0x00, 0x01}},
		{0x0000000000123456, []byte{0xfd, 0x56, 0x34, 0x12}},
		{0x0000000000ffffff, []byte{0xfd, 0xff, 0xff, 0xff}},
		{0x0000000001000000, []byte{0xfe, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00}},
		{0x123456789abcdef0, []byte{0xfe, 0xf0, 0xde, 0xbc, 0x9a, 0x78, 0x56, 0x34, 0x12}},
		{0xffffffffffffffff, []byte{0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
	}

	for _, tst := range integerTests {
		num, isNull, numLen := ReadLengthEncodedInteger(tst.encoded)
		if isNull {
			t.Errorf("%x: expected %d, got NULL", tst.encoded, tst.num)
		}
		if num != tst.num {
			t.Errorf("%x: expected %d, got %d", tst.encoded, tst.num, num)
		}
		if numLen != len(tst.encoded) {
			t.Errorf("%x: expected size %d, got %d", tst.encoded, len(tst.encoded), numLen)
		}
		encoded := AppendLengthEncodedInteger(nil, num)
		if !bytes.Equal(encoded, tst.encoded) {
			t.Errorf("%v: expected %x, got %x", num, tst.encoded, encoded)
		}
	}
}

func TestFormatBinaryDateTime(t *testing.T) {
	rawDate := [11]byte{}
	binary.LittleEndian.PutUint16(rawDate[:2], 1978)   // years
	rawDate[2] = 12                                    // months
	rawDate[3] = 30                                    // days
	rawDate[4] = 15                                    // hours
	rawDate[5] = 46                                    // minutes
	rawDate[6] = 23                                    // seconds
	binary.LittleEndian.PutUint32(rawDate[7:], 987654) // microseconds
	expect := func(expected string, inlen, outlen uint8) {
		actual, _ := FormatBinaryDateTime(rawDate[:inlen], outlen)
		bytes, ok := actual.([]byte)
		if !ok {
			t.Errorf("formatBinaryDateTime must return []byte, was %T", actual)
		}
		if string(bytes) != expected {
			t.Errorf(
				"expected %q, got %q for length in %d, out %d",
				expected, actual, inlen, outlen,
			)
		}
	}
	expect("0000-00-00", 0, 10)
	expect("0000-00-00 00:00:00", 0, 19)
	expect("1978-12-30", 4, 10)
	expect("1978-12-30 15:46:23", 7, 19)
	expect("1978-12-30 15:46:23.987654", 11, 26)
}

func TestFormatBinaryTime(t *testing.T) {
	expect := func(expected string, src []byte, outlen uint8) {
		actual, _ := FormatBinaryTime(src, outlen)
		bytes, ok := actual.([]byte)
		if !ok {
			t.Errorf("formatBinaryDateTime must return []byte, was %T", actual)
		}
		if string(bytes) != expected {
			t.Errorf(
				"expected %q, got %q for src=%q and outlen=%d",
				expected, actual, src, outlen)
		}
	}

	// binary format:
	// sign (0: positive, 1: negative), days(4), hours, minutes, seconds, micro(4)

	// Zeros
	expect("00:00:00", []byte{}, 8)
	expect("00:00:00.0", []byte{}, 10)
	expect("00:00:00.000000", []byte{}, 15)

	// Without micro(4)
	expect("12:34:56", []byte{0, 0, 0, 0, 0, 12, 34, 56}, 8)
	expect("-12:34:56", []byte{1, 0, 0, 0, 0, 12, 34, 56}, 8)
	expect("12:34:56.00", []byte{0, 0, 0, 0, 0, 12, 34, 56}, 11)
	expect("24:34:56", []byte{0, 1, 0, 0, 0, 0, 34, 56}, 8)
	expect("-99:34:56", []byte{1, 4, 0, 0, 0, 3, 34, 56}, 8)
	expect("103079215103:34:56", []byte{0, 255, 255, 255, 255, 23, 34, 56}, 8)

	// With micro(4)
	expect("12:34:56.00", []byte{0, 0, 0, 0, 0, 12, 34, 56, 99, 0, 0, 0}, 11)
	expect("12:34:56.000099", []byte{0, 0, 0, 0, 0, 12, 34, 56, 99, 0, 0, 0}, 15)
}

func TestEscapeBackslash(t *testing.T) {
	expect := func(expected, value string) {
		actual := string(escapeBytesBackslash([]byte{}, []byte(value)))
		if actual != expected {
			t.Errorf(
				"expected %s, got %s",
				expected, actual,
			)
		}

		actual = string(escapeStringBackslash([]byte{}, value))
		if actual != expected {
			t.Errorf(
				"expected %s, got %s",
				expected, actual,
			)
		}
	}

	expect("foo\\0bar", "foo\x00bar")
	expect("foo\\nbar", "foo\nbar")
	expect("foo\\rbar", "foo\rbar")
	expect("foo\\Zbar", "foo\x1abar")
	expect("foo\\\"bar", "foo\"bar")
	expect("foo\\\\bar", "foo\\bar")
	expect("foo\\'bar", "foo'bar")
}

func TestEscapeQuotes(t *testing.T) {
	expect := func(expected, value string) {
		actual := string(escapeBytesQuotes([]byte{}, []byte(value)))
		if actual != expected {
			t.Errorf(
				"expected %s, got %s",
				expected, actual,
			)
		}

		actual = string(escapeStringQuotes([]byte{}, value))
		if actual != expected {
			t.Errorf(
				"expected %s, got %s",
				expected, actual,
			)
		}
	}

	expect("foo\x00bar", "foo\x00bar") // not affected
	expect("foo\nbar", "foo\nbar")     // not affected
	expect("foo\rbar", "foo\rbar")     // not affected
	expect("foo\x1abar", "foo\x1abar") // not affected
	expect("foo''bar", "foo'bar")      // affected
	expect("foo\"bar", "foo\"bar")     // not affected
}

func TestAtomicBool(t *testing.T) {
	var ab atomicBool
	if ab.IsSet() {
		t.Fatal("Expected value to be false")
	}

	ab.Set(true)
	if ab.value != 1 {
		t.Fatal("Set(true) did not set value to 1")
	}
	if !ab.IsSet() {
		t.Fatal("Expected value to be true")
	}

	ab.Set(true)
	if !ab.IsSet() {
		t.Fatal("Expected value to be true")
	}

	ab.Set(false)
	if ab.value != 0 {
		t.Fatal("Set(false) did not set value to 0")
	}
	if ab.IsSet() {
		t.Fatal("Expected value to be false")
	}

	ab.Set(false)
	if ab.IsSet() {
		t.Fatal("Expected value to be false")
	}
	if ab.TrySet(false) {
		t.Fatal("Expected TrySet(false) to fail")
	}
	if !ab.TrySet(true) {
		t.Fatal("Expected TrySet(true) to succeed")
	}
	if !ab.IsSet() {
		t.Fatal("Expected value to be true")
	}

	ab.Set(true)
	if !ab.IsSet() {
		t.Fatal("Expected value to be true")
	}
	if ab.TrySet(true) {
		t.Fatal("Expected TrySet(true) to fail")
	}
	if !ab.TrySet(false) {
		t.Fatal("Expected TrySet(false) to succeed")
	}
	if ab.IsSet() {
		t.Fatal("Expected value to be false")
	}

	ab._noCopy.Lock() // we've "tested" it ¯\_(ツ)_/¯
}

func TestAtomicError(t *testing.T) {
	var ae atomicError
	if ae.Value() != nil {
		t.Fatal("Expected value to be nil")
	}

	ae.Set(errors.ErrMalformedPkt)
	if v := ae.Value(); v != errors.ErrMalformedPkt {
		if v == nil {
			t.Fatal("Value is still nil")
		}
		t.Fatal("Error did not match")
	}
	ae.Set(errors.ErrPktSync)
	if ae.Value() == errors.ErrMalformedPkt {
		t.Fatal("Error still matches old error")
	}
	if v := ae.Value(); v != errors.ErrPktSync {
		t.Fatal("Error did not match")
	}
}

func TestIsolationLevelMapping(t *testing.T) {
	data := []struct {
		level    driver.IsolationLevel
		expected string
	}{
		{
			level:    driver.IsolationLevel(sql.LevelReadCommitted),
			expected: "READ COMMITTED",
		},
		{
			level:    driver.IsolationLevel(sql.LevelRepeatableRead),
			expected: "REPEATABLE READ",
		},
		{
			level:    driver.IsolationLevel(sql.LevelReadUncommitted),
			expected: "READ UNCOMMITTED",
		},
		{
			level:    driver.IsolationLevel(sql.LevelSerializable),
			expected: "SERIALIZABLE",
		},
	}

	for i, td := range data {
		if actual, err := mapIsolationLevel(td.level); actual != td.expected || err != nil {
			t.Fatal(i, td.expected, actual, err)
		}
	}

	// check unsupported mapping
	expectedErr := "mysql: unsupported isolation level: 7"
	actual, err := mapIsolationLevel(driver.IsolationLevel(sql.LevelLinearizable))
	if actual != "" || err == nil {
		t.Fatal("Expected error on unsupported isolation level")
	}
	if err.Error() != expectedErr {
		t.Fatalf("Expected error to be %q, got %q", expectedErr, err)
	}
}

func TestAppendDateTime(t *testing.T) {
	tests := []struct {
		t   time.Time
		str string
	}{
		{
			t:   time.Date(1234, 5, 6, 0, 0, 0, 0, time.UTC),
			str: "1234-05-06",
		},
		{
			t:   time.Date(4567, 12, 31, 12, 0, 0, 0, time.UTC),
			str: "4567-12-31 12:00:00",
		},
		{
			t:   time.Date(2020, 5, 30, 12, 34, 0, 0, time.UTC),
			str: "2020-05-30 12:34:00",
		},
		{
			t:   time.Date(2020, 5, 30, 12, 34, 56, 0, time.UTC),
			str: "2020-05-30 12:34:56",
		},
		{
			t:   time.Date(2020, 5, 30, 22, 33, 44, 123000000, time.UTC),
			str: "2020-05-30 22:33:44.123",
		},
		{
			t:   time.Date(2020, 5, 30, 22, 33, 44, 123456000, time.UTC),
			str: "2020-05-30 22:33:44.123456",
		},
		{
			t:   time.Date(2020, 5, 30, 22, 33, 44, 123456789, time.UTC),
			str: "2020-05-30 22:33:44.123456789",
		},
		{
			t:   time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC),
			str: "9999-12-31 23:59:59.999999999",
		},
		{
			t:   time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
			str: "0001-01-01",
		},
	}
	for _, v := range tests {
		buf := make([]byte, 0, 32)
		buf, _ = AppendDateTime(buf, v.t)
		if str := string(buf); str != v.str {
			t.Errorf("appendDateTime(%v), have: %s, want: %s", v.t, str, v.str)
		}
	}

	// year out of range
	{
		v := time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC)
		buf := make([]byte, 0, 32)
		_, err := AppendDateTime(buf, v)
		if err == nil {
			t.Error("want an error")
			return
		}
	}
	{
		v := time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
		buf := make([]byte, 0, 32)
		_, err := AppendDateTime(buf, v)
		if err == nil {
			t.Error("want an error")
			return
		}
	}
}

func TestParseDateTime(t *testing.T) {
	cases := []struct {
		name string
		str  string
	}{
		{
			name: "parse date",
			str:  "2020-05-13",
		},
		{
			name: "parse null date",
			str:  sDate0,
		},
		{
			name: "parse datetime",
			str:  "2020-05-13 21:30:45",
		},
		{
			name: "parse null datetime",
			str:  sDateTime0,
		},
		{
			name: "parse datetime nanosec 1-digit",
			str:  "2020-05-25 23:22:01.1",
		},
		{
			name: "parse datetime nanosec 2-digits",
			str:  "2020-05-25 23:22:01.15",
		},
		{
			name: "parse datetime nanosec 3-digits",
			str:  "2020-05-25 23:22:01.159",
		},
		{
			name: "parse datetime nanosec 4-digits",
			str:  "2020-05-25 23:22:01.1594",
		},
		{
			name: "parse datetime nanosec 5-digits",
			str:  "2020-05-25 23:22:01.15949",
		},
		{
			name: "parse datetime nanosec 6-digits",
			str:  "2020-05-25 23:22:01.159491",
		},
	}

	for _, loc := range []*time.Location{
		time.UTC,
		time.FixedZone("test", 8*60*60),
	} {
		for _, cc := range cases {
			t.Run(cc.name+"-"+loc.String(), func(t *testing.T) {
				var want time.Time
				if cc.str != sDate0 && cc.str != sDateTime0 {
					var err error
					want, err = time.ParseInLocation(constant.TimeFormat[:len(cc.str)], cc.str, loc)
					if err != nil {
						t.Fatal(err)
					}
				}
				got, err := ParseDateTime([]byte(cc.str), loc)
				if err != nil {
					t.Fatal(err)
				}

				if !want.Equal(got) {
					t.Fatalf("want: %v, but got %v", want, got)
				}
			})
		}
	}
}

func TestParseDateTimeFail(t *testing.T) {
	cases := []struct {
		name    string
		str     string
		wantErr string
	}{
		{
			name:    "parse invalid time",
			str:     "hello",
			wantErr: "invalid time bytes: hello",
		},
		{
			name:    "parse year",
			str:     "000!-00-00 00:00:00.000000",
			wantErr: "not [0-9]",
		},
		{
			name:    "parse month",
			str:     "0000-!0-00 00:00:00.000000",
			wantErr: "not [0-9]",
		},
		{
			name:    `parse "-" after parsed year`,
			str:     "0000:00-00 00:00:00.000000",
			wantErr: "bad value for field: `:`",
		},
		{
			name:    `parse "-" after parsed month`,
			str:     "0000-00:00 00:00:00.000000",
			wantErr: "bad value for field: `:`",
		},
		{
			name:    `parse " " after parsed date`,
			str:     "0000-00-00+00:00:00.000000",
			wantErr: "bad value for field: `+`",
		},
		{
			name:    `parse ":" after parsed date`,
			str:     "0000-00-00 00-00:00.000000",
			wantErr: "bad value for field: `-`",
		},
		{
			name:    `parse ":" after parsed hour`,
			str:     "0000-00-00 00:00-00.000000",
			wantErr: "bad value for field: `-`",
		},
		{
			name:    `parse "." after parsed sec`,
			str:     "0000-00-00 00:00:00?000000",
			wantErr: "bad value for field: `?`",
		},
	}

	for _, cc := range cases {
		t.Run(cc.name, func(t *testing.T) {
			got, err := ParseDateTime([]byte(cc.str), time.UTC)
			if err == nil {
				t.Fatal("want error")
			}
			if cc.wantErr != err.Error() {
				t.Fatalf("want `%s`, but got `%s`", cc.wantErr, err)
			}
			if !got.IsZero() {
				t.Fatal("want zero time")
			}
		})
	}
}

type (
	userDefined       float64
	userDefinedSlice  []int
	userDefinedString string
)

type conversionTest struct {
	s, d interface{} // source and destination

	// following are used if they're non-zero
	wantint    int64
	wantuint   uint64
	wantstr    string
	wantbytes  []byte
	wantraw    RawBytes
	wantf32    float32
	wantf64    float64
	wanttime   time.Time
	wantbool   bool // used if d is of type *bool
	wanterr    string
	wantiface  interface{}
	wantptr    *int64 // if non-nil, *d's pointed value must be equal to *wantptr
	wantnil    bool   // if true, *d must be *int64(nil)
	wantusrdef userDefined
	wantusrstr userDefinedString
}

// Target variables for scanning into.
var (
	scanstr    string
	scanbytes  []byte
	scanraw    RawBytes
	scanint    int
	scanint8   int8
	scanint16  int16
	scanint32  int32
	scanuint8  uint8
	scanuint16 uint16
	scanbool   bool
	scanf32    float32
	scanf64    float64
	scantime   time.Time
	scanptr    *int64
	scaniface  interface{}
)

func TestConversions(t *testing.T) {
	for n, ct := range conversionTests() {
		err := convertAssignRows(ct.d, ct.s)
		errstr := ""
		if err != nil {
			errstr = err.Error()
		}
		errf := func(format string, args ...interface{}) {
			base := fmt.Sprintf("convertAssign #%d: for %v (%T) -> %T, ", n, ct.s, ct.s, ct.d)
			t.Errorf(base+format, args...)
		}
		if errstr != ct.wanterr {
			errf("got error %q, want error %q", errstr, ct.wanterr)
		}
		if ct.wantstr != "" && ct.wantstr != scanstr {
			errf("want string %q, got %q", ct.wantstr, scanstr)
		}
		if ct.wantbytes != nil && string(ct.wantbytes) != string(scanbytes) {
			errf("want byte %q, got %q", ct.wantbytes, scanbytes)
		}
		if ct.wantraw != nil && string(ct.wantraw) != string(scanraw) {
			errf("want RawBytes %q, got %q", ct.wantraw, scanraw)
		}
		if ct.wantint != 0 && ct.wantint != intValue(ct.d) {
			errf("want int %d, got %d", ct.wantint, intValue(ct.d))
		}
		if ct.wantuint != 0 && ct.wantuint != uintValue(ct.d) {
			errf("want uint %d, got %d", ct.wantuint, uintValue(ct.d))
		}
		if ct.wantf32 != 0 && ct.wantf32 != float32Value(ct.d) {
			errf("want float32 %v, got %v", ct.wantf32, float32Value(ct.d))
		}
		if ct.wantf64 != 0 && ct.wantf64 != float64Value(ct.d) {
			errf("want float32 %v, got %v", ct.wantf64, float64Value(ct.d))
		}
		if bp, boolTest := ct.d.(*bool); boolTest && *bp != ct.wantbool && ct.wanterr == "" {
			errf("want bool %v, got %v", ct.wantbool, *bp)
		}
		if !ct.wanttime.IsZero() && !ct.wanttime.Equal(timeValue(ct.d)) {
			errf("want time %v, got %v", ct.wanttime, timeValue(ct.d))
		}
		if ct.wantnil && *ct.d.(**int64) != nil {
			errf("want nil, got %v", intPtrValue(ct.d))
		}
		if ct.wantptr != nil {
			if *ct.d.(**int64) == nil {
				errf("want pointer to %v, got nil", *ct.wantptr)
			} else if *ct.wantptr != intPtrValue(ct.d) {
				errf("want pointer to %v, got %v", *ct.wantptr, intPtrValue(ct.d))
			}
		}
		if ifptr, ok := ct.d.(*interface{}); ok {
			if !reflect.DeepEqual(ct.wantiface, scaniface) {
				errf("want interface %#v, got %#v", ct.wantiface, scaniface)
				continue
			}
			if srcBytes, ok := ct.s.([]byte); ok {
				dstBytes := (*ifptr).([]byte)
				if len(srcBytes) > 0 && &dstBytes[0] == &srcBytes[0] {
					errf("copy into interface{} didn't copy []byte data")
				}
			}
		}
		if ct.wantusrdef != 0 && ct.wantusrdef != *ct.d.(*userDefined) {
			errf("want userDefined %f, got %f", ct.wantusrdef, *ct.d.(*userDefined))
		}
		if len(ct.wantusrstr) != 0 && ct.wantusrstr != *ct.d.(*userDefinedString) {
			errf("want userDefined %q, got %q", ct.wantusrstr, *ct.d.(*userDefinedString))
		}
	}
}

// Tests that assigning to RawBytes doesn't allocate (and also works).
func TestRawBytesAllocs(t *testing.T) {
	var tests = []struct {
		name string
		in   interface{}
		want string
	}{
		{"uint64", uint64(12345678), "12345678"},
		{"uint32", uint32(1234), "1234"},
		{"uint16", uint16(12), "12"},
		{"uint8", uint8(1), "1"},
		{"uint", uint(123), "123"},
		{"int", int(123), "123"},
		{"int8", int8(1), "1"},
		{"int16", int16(12), "12"},
		{"int32", int32(1234), "1234"},
		{"int64", int64(12345678), "12345678"},
		{"float32", float32(1.5), "1.5"},
		{"float64", float64(64), "64"},
		{"bool", false, "false"},
		{"time", time.Unix(2, 5).UTC(), "1970-01-01T00:00:02.000000005Z"},
	}

	buf := make(RawBytes, 10)
	test := func(name string, in interface{}, want string) {
		if err := convertAssignRows(&buf, in); err != nil {
			t.Fatalf("%s: convertAssign = %v", name, err)
		}
		match := len(buf) == len(want)
		if match {
			for i, b := range buf {
				if want[i] != b {
					match = false
					break
				}
			}
		}
		if !match {
			t.Fatalf("%s: got %q (len %d); want %q (len %d)", name, buf, len(buf), want, len(want))
		}
	}

	n := testing.AllocsPerRun(100, func() {
		for _, tt := range tests {
			test(tt.name, tt.in, tt.want)
		}
	})

	// The numbers below are only valid for 64-bit interface word sizes,
	// and gc. With 32-bit words there are more convT2E allocs, and
	// with gccgo, only pointers currently go in interface data.
	// So only care on amd64 gc for now.
	measureAllocs := runtime.GOARCH == "amd64" && runtime.Compiler == "gc"

	if n > 0.5 && measureAllocs {
		t.Fatalf("allocs = %v; want 0", n)
	}

	// This one involves a convT2E allocation, string -> interface{}
	n = testing.AllocsPerRun(100, func() {
		test("string", "foo", "foo")
	})
	if n > 1.5 && measureAllocs {
		t.Fatalf("allocs = %v; want max 1", n)
	}
}

// https://golang.org/issues/13905
func TestUserDefinedBytes(t *testing.T) {
	type userDefinedBytes []byte
	var u userDefinedBytes
	v := []byte("foo")

	convertAssignRows(&u, v)
	if &u[0] == &v[0] {
		t.Fatal("userDefinedBytes got potentially dirty driver memory")
	}
}

func conversionTests() []conversionTest {
	// Return a fresh instance to test so "go test -count 2" works correctly.
	return []conversionTest{
		// Exact conversions (destination pointer type matches source type)
		{s: "foo", d: &scanstr, wantstr: "foo"},
		{s: 123, d: &scanint, wantint: 123},
		{s: someTime, d: &scantime, wanttime: someTime},

		// To strings
		{s: "string", d: &scanstr, wantstr: "string"},
		{s: []byte("byteslice"), d: &scanstr, wantstr: "byteslice"},
		{s: 123, d: &scanstr, wantstr: "123"},
		{s: int8(123), d: &scanstr, wantstr: "123"},
		{s: int64(123), d: &scanstr, wantstr: "123"},
		{s: uint8(123), d: &scanstr, wantstr: "123"},
		{s: uint16(123), d: &scanstr, wantstr: "123"},
		{s: uint32(123), d: &scanstr, wantstr: "123"},
		{s: uint64(123), d: &scanstr, wantstr: "123"},
		{s: 1.5, d: &scanstr, wantstr: "1.5"},

		// From time.Time:
		{s: time.Unix(1, 0).UTC(), d: &scanstr, wantstr: "1970-01-01T00:00:01Z"},
		{s: time.Unix(1453874597, 0).In(time.FixedZone("here", -3600*8)), d: &scanstr, wantstr: "2016-01-26T22:03:17-08:00"},
		{s: time.Unix(1, 2).UTC(), d: &scanstr, wantstr: "1970-01-01T00:00:01.000000002Z"},
		{s: time.Time{}, d: &scanstr, wantstr: "0001-01-01T00:00:00Z"},
		{s: time.Unix(1, 2).UTC(), d: &scanbytes, wantbytes: []byte("1970-01-01T00:00:01.000000002Z")},
		{s: time.Unix(1, 2).UTC(), d: &scaniface, wantiface: time.Unix(1, 2).UTC()},

		// To []byte
		{s: nil, d: &scanbytes, wantbytes: nil},
		{s: "string", d: &scanbytes, wantbytes: []byte("string")},
		{s: []byte("byteslice"), d: &scanbytes, wantbytes: []byte("byteslice")},
		{s: 123, d: &scanbytes, wantbytes: []byte("123")},
		{s: int8(123), d: &scanbytes, wantbytes: []byte("123")},
		{s: int64(123), d: &scanbytes, wantbytes: []byte("123")},
		{s: uint8(123), d: &scanbytes, wantbytes: []byte("123")},
		{s: uint16(123), d: &scanbytes, wantbytes: []byte("123")},
		{s: uint32(123), d: &scanbytes, wantbytes: []byte("123")},
		{s: uint64(123), d: &scanbytes, wantbytes: []byte("123")},
		{s: 1.5, d: &scanbytes, wantbytes: []byte("1.5")},

		// To RawBytes
		{s: nil, d: &scanraw, wantraw: nil},
		{s: []byte("byteslice"), d: &scanraw, wantraw: RawBytes("byteslice")},
		{s: "string", d: &scanraw, wantraw: RawBytes("string")},
		{s: 123, d: &scanraw, wantraw: RawBytes("123")},
		{s: int8(123), d: &scanraw, wantraw: RawBytes("123")},
		{s: int64(123), d: &scanraw, wantraw: RawBytes("123")},
		{s: uint8(123), d: &scanraw, wantraw: RawBytes("123")},
		{s: uint16(123), d: &scanraw, wantraw: RawBytes("123")},
		{s: uint32(123), d: &scanraw, wantraw: RawBytes("123")},
		{s: uint64(123), d: &scanraw, wantraw: RawBytes("123")},
		{s: 1.5, d: &scanraw, wantraw: RawBytes("1.5")},
		// time.Time has been placed here to check that the RawBytes slice gets
		// correctly reset when calling time.Time.AppendFormat.
		{s: time.Unix(2, 5).UTC(), d: &scanraw, wantraw: RawBytes("1970-01-01T00:00:02.000000005Z")},

		// Strings to integers
		{s: "255", d: &scanuint8, wantuint: 255},
		{s: "256", d: &scanuint8, wanterr: "converting driver.Value type string (\"256\") to a uint8: value out of range"},
		{s: "256", d: &scanuint16, wantuint: 256},
		{s: "-1", d: &scanint, wantint: -1},
		{s: "foo", d: &scanint, wanterr: "converting driver.Value type string (\"foo\") to a int: invalid syntax"},

		// int64 to smaller integers
		{s: int64(5), d: &scanuint8, wantuint: 5},
		{s: int64(256), d: &scanuint8, wanterr: "converting driver.Value type int64 (\"256\") to a uint8: value out of range"},
		{s: int64(256), d: &scanuint16, wantuint: 256},
		{s: int64(65536), d: &scanuint16, wanterr: "converting driver.Value type int64 (\"65536\") to a uint16: value out of range"},

		// True bools
		{s: true, d: &scanbool, wantbool: true},
		{s: "True", d: &scanbool, wantbool: true},
		{s: "TRUE", d: &scanbool, wantbool: true},
		{s: "1", d: &scanbool, wantbool: true},
		{s: 1, d: &scanbool, wantbool: true},
		{s: int64(1), d: &scanbool, wantbool: true},
		{s: uint16(1), d: &scanbool, wantbool: true},

		// False bools
		{s: false, d: &scanbool, wantbool: false},
		{s: "false", d: &scanbool, wantbool: false},
		{s: "FALSE", d: &scanbool, wantbool: false},
		{s: "0", d: &scanbool, wantbool: false},
		{s: 0, d: &scanbool, wantbool: false},
		{s: int64(0), d: &scanbool, wantbool: false},
		{s: uint16(0), d: &scanbool, wantbool: false},

		// Not bools
		{s: "yup", d: &scanbool, wanterr: `sql/driver: couldn't convert "yup" into type bool`},
		{s: 2, d: &scanbool, wanterr: `sql/driver: couldn't convert 2 into type bool`},

		// Floats
		{s: float64(1.5), d: &scanf64, wantf64: float64(1.5)},
		{s: int64(1), d: &scanf64, wantf64: float64(1)},
		{s: float64(1.5), d: &scanf32, wantf32: float32(1.5)},
		{s: "1.5", d: &scanf32, wantf32: float32(1.5)},
		{s: "1.5", d: &scanf64, wantf64: float64(1.5)},

		// Pointers
		{s: interface{}(nil), d: &scanptr, wantnil: true},
		{s: int64(42), d: &scanptr, wantptr: &answer},

		// To interface{}
		{s: float64(1.5), d: &scaniface, wantiface: float64(1.5)},
		{s: int64(1), d: &scaniface, wantiface: int64(1)},
		{s: "str", d: &scaniface, wantiface: "str"},
		{s: []byte("byteslice"), d: &scaniface, wantiface: []byte("byteslice")},
		{s: true, d: &scaniface, wantiface: true},
		{s: nil, d: &scaniface},
		{s: []byte(nil), d: &scaniface, wantiface: []byte(nil)},

		// To a user-defined type
		{s: 1.5, d: new(userDefined), wantusrdef: 1.5},
		{s: int64(123), d: new(userDefined), wantusrdef: 123},
		{s: "1.5", d: new(userDefined), wantusrdef: 1.5},
		{s: []byte{1, 2, 3}, d: new(userDefinedSlice), wanterr: `unsupported Scan, storing driver.Value type []uint8 into type *misc.userDefinedSlice`},
		{s: "str", d: new(userDefinedString), wantusrstr: "str"},

		// Other errors
		{s: complex(1, 2), d: &scanstr, wanterr: `unsupported Scan, storing driver.Value type complex128 into type *string`},
	}
}

func intPtrValue(intptr interface{}) interface{} {
	return reflect.Indirect(reflect.Indirect(reflect.ValueOf(intptr))).Int()
}

func intValue(intptr interface{}) int64 {
	return reflect.Indirect(reflect.ValueOf(intptr)).Int()
}

func uintValue(intptr interface{}) uint64 {
	return reflect.Indirect(reflect.ValueOf(intptr)).Uint()
}

func float64Value(ptr interface{}) float64 {
	return *(ptr.(*float64))
}

func float32Value(ptr interface{}) float32 {
	return *(ptr.(*float32))
}

func timeValue(ptr interface{}) time.Time {
	return *(ptr.(*time.Time))
}
