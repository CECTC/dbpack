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
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/dop251/goja"
	"github.com/golang-module/carbon"
	"github.com/pkg/errors"

	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/misc"
	"github.com/cectc/dbpack/third_party/parser/opcode"
)

const (
	funcUnary = "$_unary"

	// string func
	funcAscii           = "$_ASCII"
	funcLength          = "$_LENGTH"
	funcCharLength      = "$_CHAR_LENGTH"
	funcCharacterLength = "$_CHARACTER_LENGTH"
	funcConcat          = "$_CONCAT"
	funcConcatWs        = "$_CONCAT_WS"
	funcField           = "$_FIELD"
	funcFindInSet       = "$_FIND_IN_SET"
	funcFormat          = "$_FORMAT"
	funcInsert          = "$_INSERT"
	funcLocate          = "$_LOCATE"
	funcLCase           = "$_LCASE"
	funcLeft            = "$_LEFT"
	funcLower           = "$_LOWER"
	funcLPad            = "$_LPAD"
	funcLTrim           = "$_LTRIM"
	funcMID             = "$_MID"
	funcPosition        = "$_POSITION"
	funcRepeat          = "$_REPEAT"
	funcReplace         = "$_REPLACE"
	funcReverse         = "$_REVERSE"
	funcRight           = "$_RIGHT"
	funcRPad            = "$_RPAD"
	funcRTrim           = "$_RTRIM"
	funcSpace           = "$_SPACE"
	funcStrCmp          = "$_STRCMP"
	funcSubStr          = "$_SUBSTR"
	funcSubString       = "$_SUBSTRING"
	funcSubstringIndex  = "$_SUBSTRING_INDEX"
	funcTrim            = "$_TRIM"
	funcUCase           = "$_UCASE"
	funcUpper           = "$_UPPER"

	// number func
	funcAbs   = "$_ABS"
	funcACos  = "$_ACOS"
	funcASin  = "$_ASIN"
	funcATan  = "$_ATAN"
	funcATan2 = "$_ATAN2"
	// funcAvg discard
	funcAvg     = "$_AVG"
	funcCeil    = "$_CEIL"
	funcCeiling = "$_CEILING"
	funcCos     = "$_COS"
	// funcCount discard
	funcCount    = "$_COUNT"
	funcDegrees  = "$_DEGREES"
	funcEXP      = "$_EXP"
	funcFloor    = "$_FLOOR"
	funcGreatest = "$_GREATEST"
	funcLeast    = "$_LEAST"
	funcLN       = "$_LN"
	funcLog      = "$_LOG"
	funcLog10    = "$_LOG10"
	funcLog2     = "$_LOG2"
	funcMax      = "$_MAX"
	funcMin      = "$_MIN"
	funcMod      = "$_MOD"
	funcPI       = "$_PI"
	funcPow      = "$_POW"
	funcPower    = "$_POWER"
	funcRadians  = "$_RADIANS"
	funcRand     = "$_RAND"
	funcRound    = "$_ROUND"
	funcSign     = "$_SIGN"
	funcSin      = "$_SIN"
	funcSqrt     = "$_SQRT"
	funcSum      = "$_SUM"
	funcTan      = "$_TAN"
	funcTruncate = "$_TRUNCATE"

	// date func
	funcAddDate          = "$_ADDDATE"
	funcAddTime          = "$_ADDTIME"
	funcCurDate          = "$_CURDATE"
	funcCurrentDate      = "$_CURRENT_DATE"
	funcCurrentTime      = "$_CURRENT_TIME"
	funcCurrentTimestamp = "$_CURRENT_TIMESTAMP"
	funcCurTime          = "$_CURTIME"
	funcDate             = "$_DATE"
	funcDateDiff         = "$_DATEDIFF"
	funcDateAdd          = "$_DATE_ADD"
	funcDateFormat       = "$_DATE_FORMAT"
	funcDateSub          = "$_DATE_SUB"
	funcDay              = "$_DAY"
	funcDayName          = "$_DAYNAME"
	funcDayOfMonth       = "$_DAYOFMONTH"
	funcDayOfWeek        = "$_DAYOFWEEK"
	funcDayOfYear        = "$_DAYOFYEAR"
	funcFromUnixTime     = "$_FROM_UNIXTIME"
	// funcExtract discard
	funcExtract        = "$_EXTRACT"
	funcFromDays       = "$_FROM_DAYS"
	funcHour           = "$_HOUR"
	funcLastDay        = "$_LAST_DAY"
	funcLocalTime      = "$_LOCALTIME"
	funcLocalTimestamp = "$_LOCALTIMESTAMP"
	funcMakeYear       = "$_MAKEDATE"
	funcMakeTime       = "$_MAKETIME"
	funcMicroSecond    = "$_MICROSECOND"
	funcMinute         = "$_MINUTE"
	funcMonthName      = "$_MONTHNAME"
	funcMonth          = "$_MONTH"
	funcNow            = "$_NOW"
	funcPeriodAdd      = "$_PERIOD_ADD"
	funcPeriodDiff     = "$_PERIOD_DIFF"
	funcQuarter        = "$_QUARTER"
	funcSecond         = "$_SECOND"
	funcSecToTime      = "$_SEC_TO_TIME"
	funcStrToDate      = "$_STR_TO_DATE"
	funcSubDate        = "$_SUBDATE"
	funcSubTime        = "$_SUBTIME"
	funcSysDate        = "$_SYSDATE"
	funcUnixTimestamp  = "$_UNIX_TIMESTAMP"
	funcTime           = "$_TIME"
	funcTimeFormat     = "$_TIME_FORMAT"
	funcTimeToSec      = "$_TIME_TO_SEC"
	funcTimeDiff       = "$_TIMEDIFF"
	funcTimestamp      = "$_TIMESTAMP"
	funcTimestampDiff  = "$_TIMESTAMPDIFF"
	funcToDays         = "$_TO_DAYS"
	funcWeek           = "$_WEEK"
	funcWeekDay        = "$_WEEKDAY"
	funcWeekOfYear     = "$_WEEKOFYEAR"
	funcYear           = "$_YEAR"
	funcYearWeek       = "$_YEARWEEK"

	// other func
	funcCoalesce = "$_COALESCE"
	funcIf       = "$_IF"
	funcIfNull   = "$_IFNULL"
	funcMd5      = "$_MD5"
	funcSHA      = "$_SHA"
	funcSHA1     = "$_SHA1"

	// cast func
	funcCastUnsigned = "$_CAST_UNSIGNED"
	funcCastSigned   = "$_CAST_SIGNED"
	funcCastCharset  = "$_CAST_CHARSET"
	funcCastNChar    = "$_CAST_NCHAR"
	funcCastChar     = "$_CAST_CHAR"
	funcCastDate     = "$_CAST_DATE"
	funcCastDateTime = "$_CAST_DATETIME"
	funcCastTime     = "$_CAST_TIME"
	funcCastDecimal  = "$_CAST_DECIMAL"
)

var (
	_decimalRegex = regexp.MustCompile(`^(?P<sign>[+\-])?(?P<l>[0-9])+(?P<r>\.[0-9]+)$`)
	freeList      = make(chan *JsRuntime, 16)
)

type JsRuntime struct {
	*goja.Runtime
	registered map[uint64]struct{}
}

func NewJSRuntime() *JsRuntime {
	runtime := &JsRuntime{
		Runtime:    goja.New(),
		registered: make(map[uint64]struct{}),
	}
	if err := runtime.registerUnary(); err != nil {
		log.Fatal(err)
	}
	if err := runtime.registerToString(); err != nil {
		log.Fatal(err)
	}
	if err := runtime.registerParseTime(); err != nil {
		log.Fatal(err)
	}
	if err := runtime.registerStringFunc(); err != nil {
		log.Fatal(err)
	}
	if err := runtime.registerNumberFunc(); err != nil {
		log.Fatal(err)
	}
	if err := runtime.registerTimeFunc(); err != nil {
		log.Fatal(err)
	}
	if err := runtime.registerOtherFunc(); err != nil {
		log.Fatal(err)
	}
	return runtime
}

func BorrowJsRuntime() *JsRuntime {
	var js *JsRuntime
	select {
	case js = <-freeList:
	default:
		js = NewJSRuntime()
	}
	return js
}

func ReturnJsRuntime(js *JsRuntime) {
	select {
	case freeList <- js:
	default:
	}
}

func (js *JsRuntime) Eval(script string, args []interface{}) (interface{}, error) {
	id := xxhash.Sum64String(script)

	if _, ok := js.registered[id]; !ok {
		if err := js.register(id, script); err != nil {
			return nil, err
		}
		js.registered[id] = struct{}{}
	}

	fnName := js.jsFuncName(id)
	arguments := make([]goja.Value, 0, len(args))

	for _, it := range args {
		arguments = append(arguments, js.ToValue(it))
	}

	fn, ok := goja.AssertFunction(js.Get(fnName))
	if !ok {
		return nil, errors.Errorf("must register func first")
	}

	v, err := fn(goja.Undefined(), arguments...)
	if err != nil {
		return nil, errors.Wrapf(err, "eval the js script '%s' failed: arguments=%v", script, args)
	}

	if goja.IsNaN(v) || goja.IsInfinity(v) {
		return math.NaN(), nil
	}

	return v.Export(), nil
}

func (js *JsRuntime) register(id uint64, script string) error {
	var sb strings.Builder

	sb.WriteString("function ")
	sb.WriteString("$_")
	sb.WriteString(strconv.FormatUint(id, 16))
	sb.WriteString("() {\n  return ")
	sb.WriteString(script)
	sb.WriteString(";\n}")

	_, err := js.RunString(sb.String())
	return err
}

func (js *JsRuntime) jsFuncName(id uint64) string {
	var sb strings.Builder
	sb.WriteString("$_")
	sb.WriteString(strconv.FormatUint(id, 16))
	return sb.String()
}

func (js *JsRuntime) registerUnary() error {
	return js.Set(funcUnary, func(op opcode.Op, v interface{}) goja.Value {
		v, err := misc.ComputeUnary(op, v)
		if err != nil {
			return goja.Null()
		}
		return js.ToValue(v)
	})
}

func (js *JsRuntime) registerToString() error {
	return js.Set("$_toString", func(v interface{}) goja.Value {
		s, ok := toString(v)
		if !ok {
			return goja.Null()
		}
		return js.ToValue(s)
	})
}

func (js *JsRuntime) registerParseTime() error {
	return js.Set("$_parseTime", func(t string) goja.Value {
		cb, err := parseCarbon(t)
		if err != nil {
			return goja.Null()
		}

		return js.ToValue(cb.TimestampMilli())
	})
}

func parseCarbon(input interface{}) (carbon.Carbon, error) {
	switch val := input.(type) {
	case time.Time:
		return carbon.Time2Carbon(val), nil
	case string:
		return carbon.Parse(val), nil
	case int64:
		return carbon.CreateFromTimestamp(val), nil
	}
	return carbon.NewCarbon(), errors.Errorf("unable to parse time, %v", input)
}

func toString(input interface{}) (string, bool) {
	if input == nil {
		return "", false
	}

	switch v := input.(type) {
	case string:
		return v, true
	case time.Time:
		return v.UTC().Format("2006-01-02 15:04:05"), true
	}

	return fmt.Sprintf("%v", input), true
}
