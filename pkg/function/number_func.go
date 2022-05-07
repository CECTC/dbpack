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

import "fmt"

func (js *JsRuntime) registerNumberFunc() error {
	if err := js.registerAbs(); err != nil {
		return err
	}
	if err := js.registerCeil(); err != nil {
		return err
	}
	if err := js.registerFloor(); err != nil {
		return err
	}
	if err := js.registerRound(); err != nil {
		return err
	}
	if err := js.registerRand(); err != nil {
		return err
	}
	if err := js.registerPow(); err != nil {
		return err
	}
	if err := js.registerPower(); err != nil {
		return err
	}
	if err := js.registerSqrt(); err != nil {
		return err
	}
	if err := js.registerExp(); err != nil {
		return err
	}
	if err := js.registerMod(); err != nil {
		return err
	}
	if err := js.registerSign(); err != nil {
		return err
	}
	if err := js.registerPI(); err != nil {
		return err
	}
	if err := js.registerTruncate(); err != nil {
		return err
	}
	return nil
}

func (js *JsRuntime) registerAbs() error {
	script := fmt.Sprintf(`function %s(n) {
				if (n === null || n === undefined) return null;
    			return Math.abs(n);
			}`, funcAbs)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerCeil() error {
	script := fmt.Sprintf(`function %s(n) {
				if (n === null || n === undefined) return null;
    			return Math.ceil(n);
			}`, funcCeil)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerFloor() error {
	script := fmt.Sprintf(`function %s(n) {
				if (n === null || n === undefined) return null;
    			return Math.floor(n);
			}`, funcFloor)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerRound() error {
	script := fmt.Sprintf(`function %s(n) {
				if (n === null || n === undefined) return null;
    			return Math.round(n);
			}`, funcRound)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerRand() error {
	script := fmt.Sprintf(`function %s() {
				return Math.random();
			}`, funcRand)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerPow() error {
	script := fmt.Sprintf(`function %s(x, y) {
				if (x === null || x === undefined) return null;
    			return Math.pow(x, y);
			}`, funcPow)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerPower() error {
	script := fmt.Sprintf(`function %s(x, y) {
				if (x === null || x === undefined) return null;
    			return Math.pow(x, y);
			}`, funcPower)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerSqrt() error {
	script := fmt.Sprintf(`function %s(n) {
				if (n === null || n === undefined) return null;
    			return Math.sqrt(n);
			}`, funcSqrt)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerExp() error {
	script := fmt.Sprintf(`function %s(n) {
				if (n === null || n === undefined) return null;
    			return Math.exp(n);
			}`, funcEXP)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerSign() error {
	script := fmt.Sprintf(`function %s(n) {
				if (n === null || n === undefined) return null;
    			return Math.sign(n);
			}`, funcSign)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerPI() error {
	script := fmt.Sprintf(`function %s(n) {
				return Math.PI;
			}`, funcPI)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerTruncate() error {
	script := fmt.Sprintf(`function %s(x, y) {
				if (x === null || x === undefined) return null;
				let n = parseInt(y) * 100;
				return Math.floor(x * n) / n;
			}`, funcTruncate)
	_, err := js.RunString(script)
	return err
}

func (js *JsRuntime) registerMod() error {
	script := fmt.Sprintf(`function %s(x, y) {
				if (x === null || x === undefined) return null;
    			return x %% y;
			}`, funcMod)
	_, err := js.RunString(script)
	return err
}
