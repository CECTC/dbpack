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
	"crypto/md5"
	"crypto/sha1"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/dop251/goja"
)

func (js *JsRuntime) registerOtherFunc() error {
	if err := js.registerIf(); err != nil {
		return err
	}
	if err := js.registerIfNull(); err != nil {
		return err
	}
	if err := js.registerSHA(); err != nil {
		return err
	}
	if err := js.registerSHA1(); err != nil {
		return err
	}
	if err := js.registerMD5(); err != nil {
		return err
	}
	if err := js.registerCastUnsigned(); err != nil {
		return err
	}
	if err := js.registerCastSigned(); err != nil {
		return err
	}
	if err := js.registerCastCharset(); err != nil {
		return err
	}
	if err := js.registerCastNChar(); err != nil {
		return err
	}
	if err := js.registerCastChar(); err != nil {
		return err
	}
	if err := js.registerCastDate(); err != nil {
		return err
	}
	if err := js.registerCastDateTime(); err != nil {
		return err
	}
	if err := js.registerCastTime(); err != nil {
		return err
	}
	if err := js.registerCastDecimal(); err != nil {
		return err
	}
	return nil
}

func (js *JsRuntime) registerIf() error {
	script := fmt.Sprintf(`function %s(a, b, c) {
				if (typeof a === "string") {
					return a === "0" ? c : b;
				}
				return a ? b : c;
			}`, funcIf)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerIfNull() error {
	script := fmt.Sprintf(`function %s(a, b) {
				if (a === null || a === undefined) {
					return b;
				}
				return a;
			}`, funcIfNull)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerSHA() error {
	return js.Set(funcSHA, func(src interface{}) goja.Value {
		s, ok := toString(src)
		if !ok {
			return goja.Null()
		}
		hash := sha1.New()
		_, _ = io.WriteString(hash, s)
		return js.ToValue(fmt.Sprintf("%x", hash.Sum(nil)))
	})
}

func (js *JsRuntime) registerSHA1() error {
	return js.Set(funcSHA1, func(src interface{}) goja.Value {
		s, ok := toString(src)
		if !ok {
			return goja.Null()
		}
		hash := sha1.New()
		_, _ = io.WriteString(hash, s)
		return js.ToValue(fmt.Sprintf("%x", hash.Sum(nil)))
	})
}

func (js *JsRuntime) registerMD5() error {
	return js.Set(funcMd5, func(src interface{}) goja.Value {
		s, ok := toString(src)
		if !ok {
			return goja.Null()
		}
		hash := md5.New()
		_, _ = io.WriteString(hash, s)
		return js.ToValue(fmt.Sprintf("%x", hash.Sum(nil)))
	})
}

func (js *JsRuntime) registerCastUnsigned() error {
	script := fmt.Sprintf(`function %s(s) {
				let x = parseInt(s)
				if (x >= 0) {
					return x;
				}
				return $_unary('~', x)
			}`, funcCastUnsigned)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerCastSigned() error {
	script := fmt.Sprintf(`function %s(s) {
				return parseInt(s);
			}`, funcCastSigned)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerCastCharset() error {
	return js.Set(funcCastCharset, func(input interface{}) goja.Value {
		s, ok := toString(input)
		if !ok {
			return goja.Null()
		}
		switch strings.ToUpper(s) {
		case "UTF8":
		}
		return js.ToValue(s)
	})
}

func (js *JsRuntime) registerCastNChar() error {
	script := fmt.Sprintf(`function %s(s) {
				if (typeof s !== 'string') {
					s = $_toString(s)
				}
				if (s === null) return null;
				return s;
			}`, funcCastNChar)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerCastChar() error {
	script := fmt.Sprintf(`function %s(length, charset, src) {
				if (charset === '') {
					if (typeof src !== 'string') {
						src = $_toString(src)
					}
				} else {
					src = $_CAST_CHARSET(src)
				}
			
				if (src === null) return null;
				return length > 0 ? src.substring(0, length) : src;
			}`, funcCastChar)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerCastDate() error {
	script := fmt.Sprintf(`function %s(s) {
				if (s instanceof Date) {
					s.setHours(0, 0, 0, 0);
					return s;
				}
				if (typeof s === 'string') {
					var ts = $_parseTime(s)
					if (ts === null) return null;
					var d = new Date(ts)
					d.setHours(0, 0, 0, 0);
					return d
				}
				return null;
			}`, funcCastDate)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerCastDateTime() error {
	script := fmt.Sprintf(`function %s(s) {
				if (s instanceof Date) {
					return s;
				}
				if (typeof s === 'string') {
					let ts = $_parseTime(s)
					if (ts === null) return null;
					return new Date(ts)
				}
				return null;
			}`, funcCastDateTime)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerCastTime() error {
	script := fmt.Sprintf(`function %s(s) {
				if (s instanceof Date) {
					return $_LPAD(s.getHours(), 2, '0') + ':' + $_LPAD(s.getMinutes(), 2, '0') + ':' + $_LPAD(s.getSeconds(), 2, '0')
				}
				if (typeof s === 'string') {
					let ts = $_parseTime(s)
					if (ts !== null) {
						s = new Date(ts)
						return $_LPAD(s.getHours(), 2, '0') + ':' + $_LPAD(s.getMinutes(), 2, '0') + ':' + $_LPAD(s.getSeconds(), 2, '0')
					}
				}
				return null;
			}`, funcCastTime)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerCastDecimal() error {
	return js.Set(funcCastDecimal, func(m, d int, s string) goja.Value {
		if !_decimalRegex.MatchString(s) {
			return goja.Null()
		}

		subs := _decimalRegex.FindStringSubmatch(s)
		keys := _decimalRegex.SubexpNames()
		if len(subs) != len(keys) {
			return goja.Null()
		}

		var (
			neg   bool
			left  string
			right string
		)
		for i := 1; i < len(keys); i++ {
			sub := subs[i]
			switch keys[i] {
			case "sign":
				if sub == "-" {
					neg = true
				}
			case "l":
				left = sub
			case "r":
				right = sub[1:]
			}
		}

		// TODO: handle overflow
		if len(left) > m-d {
			return goja.Null()
		}

		if len(right) > d {
			right = right[:d]
		}

		var sb strings.Builder
		if neg {
			sb.WriteByte('-')
		}
		sb.WriteString(left)
		if strings.TrimRight(right, "0") != "" {
			sb.WriteByte('.')
			sb.WriteString(right)
		}
		f, err := strconv.ParseFloat(sb.String(), 64)
		if err != nil {
			return goja.Null()
		}
		return js.ToValue(f)
	})
}
