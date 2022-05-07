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
	"unicode"

	"github.com/cectc/dbpack/pkg/misc"
)

func (js *JsRuntime) registerStringFunc() error {
	if err := js.registerLength(); err != nil {
		return err
	}
	if err := js.registerCharLength(); err != nil {
		return err
	}
	if err := js.registerUpper(); err != nil {
		return err
	}
	if err := js.registerLower(); err != nil {
		return err
	}
	if err := js.registerLeft(); err != nil {
		return err
	}
	if err := js.registerRight(); err != nil {
		return err
	}
	if err := js.registerLeftTrim(); err != nil {
		return err
	}
	if err := js.registerRightTrim(); err != nil {
		return err
	}
	if err := js.registerLeftPad(); err != nil {
		return err
	}
	if err := js.registerRightPad(); err != nil {
		return err
	}
	if err := js.registerSubString(); err != nil {
		return err
	}
	if err := js.registerReverse(); err != nil {
		return err
	}
	if err := js.registerRepeat(); err != nil {
		return err
	}
	if err := js.registerConcat(); err != nil {
		return err
	}
	if err := js.registerConcatWs(); err != nil {
		return err
	}
	if err := js.registerSpace(); err != nil {
		return err
	}
	if err := js.registerReplace(); err != nil {
		return err
	}
	if err := js.registerStrCmp(); err != nil {
		return err
	}
	return nil
}

func (js *JsRuntime) registerLength() error {
	script := fmt.Sprintf(`function %s(s) {
				let b = 0;
				for (let i = 0; i < s.length; i++) {
					if (s.charCodeAt(i) > 255) {
						b += 3; // UTF8 as 3 byte
					} else {
						b++;
					}
				}
				return b;
			}`, funcLength)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerCharLength() error {
	script := fmt.Sprintf(`function %s(s) {
				return s.length;
			}`, funcCharLength)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerUpper() error {
	script := fmt.Sprintf(`function %s(s) {
				return s.toUpperCase();
			}`, funcUpper)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerLower() error {
	script := fmt.Sprintf(`function %s(s) {
				return s.toLowerCase();
			}`, funcLower)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerLeft() error {
	script := fmt.Sprintf(`function %s(s, n) {
				return s.substring(0, n);
			}`, funcLeft)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerRight() error {
	script := fmt.Sprintf(`function %s(s, n) {
				return s.substring(s.length - n, s.length);
			}`, funcRight)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerLeftTrim() error {
	return js.Set(funcLTrim, func(s string) string {
		return strings.TrimLeftFunc(s, func(r rune) bool {
			return unicode.IsSpace(r)
		})
	})
}

func (js *JsRuntime) registerRightTrim() error {
	return js.Set(funcRTrim, func(s string) string {
		return strings.TrimRightFunc(s, func(r rune) bool {
			return unicode.IsSpace(r)
		})
	})
}

func (js *JsRuntime) registerLeftPad() error {
	return js.Set(funcLPad, func(s string, n int, pad string) string {
		return misc.PadLeft(s, pad, n)
	})
}

func (js *JsRuntime) registerRightPad() error {
	return js.Set(funcRPad, func(s string, n int, pad string) string {
		return misc.PadRight(s, pad, n)
	})
}

func (js *JsRuntime) registerSubString() error {
	script := fmt.Sprintf(`function %s(s, n, len) {
				if (n === 0 || len < 1) return "";
				if (n > 0) return s.substring(n - 1, n - 1 + len)
				return s.substring(s.length + n, s.length + n + len)
			}`, funcSubString)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerRepeat() error {
	script := fmt.Sprintf(`function %s(s, n) {
				let ret = "";
				for (let i = 0; i < n; i++) {
					ret += s;
				}
				return ret;
			}`, funcRepeat)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerReverse() error {
	script := fmt.Sprintf(`function %s(s) {
				return s.split("").reverse().join("");
			}`, funcReverse)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerConcat() error {
	script := fmt.Sprintf(`function %s() {
				let s = "";
				for (let i = 0; i < arguments.length; i++) {
					s += arguments[i]
				}
				return s;
			}`, funcConcat)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerConcatWs() error {
	script := fmt.Sprintf(`function %s() {
				if (arguments.length < 2) {
					return "";
				}
				let s = "" + arguments[1];
				for (let i = 2; i < arguments.length; i++) {
					s += arguments[0];
					s += arguments[i];
				}
				return s;
			}`, funcConcatWs)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerSpace() error {
	script := fmt.Sprintf(`function %s(n) {
				let ret = "";
				for (let i = 0; i < n; i++) {
					ret += " ";
				}
				return ret;
			}`, funcSpace)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerReplace() error {
	script := fmt.Sprintf(`function %s(s, search, replace) {
				return s.replace(new RegExp(search, 'g'), replace);
			}`, funcReplace)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerStrCmp() error {
	script := fmt.Sprintf(`function %s(a, b) {
				if (a === b) {
					return 0;
				}
				if (a > b) {
					return 1;
				}
				return -1;
			}`, funcStrCmp)
	_, err := js.RunString(script)
	return err
}
