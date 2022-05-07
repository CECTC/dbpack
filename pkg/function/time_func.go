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

	"github.com/dop251/goja"
	"github.com/golang-module/carbon"
)

func (js *JsRuntime) registerTimeFunc() error {
	if err := js.registerNow(); err != nil {
		return err
	}
	if err := js.registerCurDate(); err != nil {
		return err
	}
	if err := js.registerCurrentDate(); err != nil {
		return err
	}
	if err := js.registerSysDate(); err != nil {
		return err
	}
	if err := js.registerUnixTimestamp(); err != nil {
		return err
	}
	if err := js.registerCurrentTimestamp(); err != nil {
		return err
	}
	if err := js.registerCurrentTime(); err != nil {
		return err
	}
	if err := js.registerLocalTime(); err != nil {
		return err
	}
	if err := js.registerQuarter(); err != nil {
		return err
	}
	if err := js.registerMonthName(); err != nil {
		return err
	}
	if err := js.registerMonth(); err != nil {
		return err
	}
	if err := js.registerDayName(); err != nil {
		return err
	}
	if err := js.registerDay(); err != nil {
		return err
	}
	if err := js.registerDayOfWeek(); err != nil {
		return err
	}
	if err := js.registerDayOfMonth(); err != nil {
		return err
	}
	if err := js.registerDayOfYear(); err != nil {
		return err
	}
	if err := js.registerHour(); err != nil {
		return err
	}
	if err := js.registerMinute(); err != nil {
		return err
	}
	if err := js.registerSecond(); err != nil {
		return err
	}
	if err := js.registerAddDate(); err != nil {
		return err
	}
	if err := js.registerSubDate(); err != nil {
		return err
	}
	if err := js.registerDateDiff(); err != nil {
		return err
	}
	if err := js.registerFromUnixTime(); err != nil {
		return err
	}
	if err := js.registerDateFormat(); err != nil {
		return err
	}
	return nil
}

// registerNow funcNow
func (js *JsRuntime) registerNow() error {
	script := fmt.Sprintf(`function %s() {
				return new Date();
			}`, funcNow)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerCurDate() error {
	return js.Set(funcCurDate, func() string {
		return carbon.Now().ToDateString()
	})
}

func (js *JsRuntime) registerCurrentDate() error {
	return js.Set(funcCurrentDate, func() string {
		return carbon.Now().ToDateString()
	})
}

// registerSysDate funcSysDate
func (js *JsRuntime) registerSysDate() error {
	script := fmt.Sprintf(`function %s() {
				return new Date();
			}`, funcSysDate)
	_, err := js.RunString(script)
	return err
}

// registerUnixTimestamp funcUnixTimestamp
func (js *JsRuntime) registerUnixTimestamp() error {
	script := fmt.Sprintf(`function %s() {
				return ~~(Date.now() / 1000);
			}`, funcUnixTimestamp)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerCurrentTimestamp() error {
	script := fmt.Sprintf(`function %s() {
				return new Date();
			}`, funcCurrentTimestamp)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerCurrentTime() error {
	return js.Set(funcCurTime, func() string {
		return carbon.Now().ToTimeString()
	})
}

func (js *JsRuntime) registerLocalTime() error {
	script := fmt.Sprintf(`function %s() {
				return new Date();
			}`, funcLocalTime)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerQuarter() error {
	return js.Set(funcQuarter, func(t interface{}) goja.Value {
		cb, err := parseCarbon(t)
		if err != nil {
			return goja.Null()
		}
		return js.ToValue(cb.Quarter())
	})
}

// registerMonthName MONTHNAME(d)
func (js *JsRuntime) registerMonthName() error {
	return js.Set(funcMonthName, func(t interface{}) goja.Value {
		cb, err := parseCarbon(t)
		if err != nil {
			return goja.Null()
		}

		return js.ToValue(cb.ToMonthString())
	})
}

// registerMonth MONTH(d)
func (js *JsRuntime) registerMonth() error {
	return js.Set(funcMonth, func(t interface{}) goja.Value {
		cb, err := parseCarbon(t)
		if err != nil {
			return goja.Null()
		}
		return js.ToValue(cb.Month())
	})
}

func (js *JsRuntime) registerDayName() error {
	return js.Set(funcDayName, func(t interface{}) goja.Value {
		cb, err := parseCarbon(t)
		if err != nil {
			return goja.Null()
		}
		return js.ToValue(cb.ToWeekString())
	})
}

func (js *JsRuntime) registerDay() error {
	return js.Set(funcDay, func(t interface{}) goja.Value {
		cb, err := parseCarbon(t)
		if err != nil {
			return goja.Null()
		}
		return js.ToValue(cb.Day())
	})
}

func (js *JsRuntime) registerDayOfWeek() error {
	return js.Set(funcDayOfWeek, func(t interface{}) goja.Value {
		cb, err := parseCarbon(t)
		if err != nil {
			return goja.Null()
		}
		return js.ToValue(cb.DayOfWeek())
	})
}

func (js *JsRuntime) registerDayOfMonth() error {
	return js.Set(funcDayOfMonth, func(t interface{}) goja.Value {
		cb, err := parseCarbon(t)
		if err != nil {
			return goja.Null()
		}
		return js.ToValue(cb.DayOfMonth())
	})
}

func (js *JsRuntime) registerDayOfYear() error {
	return js.Set(funcDayOfYear, func(t interface{}) goja.Value {
		cb, err := parseCarbon(t)
		if err != nil {
			return goja.Null()
		}
		return js.ToValue(cb.DayOfYear())
	})
}

func (js *JsRuntime) registerHour() error {
	return js.Set(funcHour, func(t interface{}) goja.Value {
		cb, err := parseCarbon(t)
		if err != nil {
			return goja.Null()
		}
		return js.ToValue(cb.Hour())
	})
}

func (js *JsRuntime) registerMinute() error {
	return js.Set(funcMinute, func(t interface{}) goja.Value {
		cb, err := parseCarbon(t)
		if err != nil {
			return goja.Null()
		}
		return js.ToValue(cb.Minute())
	})
}

// registerSecond return second in t
func (js *JsRuntime) registerSecond() error {
	return js.Set(funcSecond, func(t interface{}) goja.Value {
		cb, err := parseCarbon(t)
		if err != nil {
			return goja.Null()
		}
		return js.ToValue(cb.Second())
	})
}

func (js *JsRuntime) registerAddDate() error {
	return js.Set(funcAddDate, func(t interface{}, days int) goja.Value {
		cb, err := parseCarbon(t)
		if err != nil {
			return goja.Null()
		}

		return js.ToValue(cb.AddDays(days).Time)
	})
}

func (js *JsRuntime) registerSubDate() error {
	script := fmt.Sprintf(`function %s(d, n) {
    			return new Date($_ADDDATE(d, -n));
			}`, funcSubDate)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerDateDiff() error {
	return js.Set(funcDateDiff, func(t1, t2 interface{}) goja.Value {
		cb1, err := parseCarbon(t1)
		if err != nil {
			return goja.Null()
		}
		cb2, err := parseCarbon(t2)
		if err != nil {
			return goja.Null()
		}
		return js.ToValue(cb1.DiffInDays(cb2))
	})
}

func (js *JsRuntime) registerFromUnixTime() error {
	script := fmt.Sprintf(`function %s(ts) {
				if (ts === null || ts === undefined) return null;
				let n = -1;
				if (typeof ts === 'string') {
					n = parseInt(ts)
				} else if (typeof ts === 'number') {
					n = ts;
				} else if (typeof ts === 'boolean') {
					n = ts ? 1 : 0;
				}
				if (n < 0) return null;
				return new Date(1000 * n);
			}`, funcFromUnixTime)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerDateFormat() error {
	return js.Set(funcDateFormat, func(t interface{}, format string) goja.Value {
		cb, err := parseCarbon(t)
		if err != nil {
			return goja.Null()
		}
		return js.ToValue(cb.Format(format))
	})
}
