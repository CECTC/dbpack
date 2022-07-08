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

	"github.com/cectc/dbpack/third_party/parser/ast"
	"github.com/cectc/dbpack/third_party/parser/model"
)

const (
	XIDHint         = "XID"
	GlobalLockHint  = "GlobalLock"
	TraceParentHint = "TraceParent"
)

func HasXIDHint(hints []*ast.TableOptimizerHint) (bool, string) {
	for _, hint := range hints {
		if strings.EqualFold(hint.HintName.String(), XIDHint) {
			hintData := hint.HintData.(model.CIStr)
			xid := hintData.String()
			return true, xid
		}
	}
	return false, ""
}

func HasGlobalLockHint(hints []*ast.TableOptimizerHint) bool {
	for _, hint := range hints {
		if strings.EqualFold(hint.HintName.String(), GlobalLockHint) {
			return true
		}
	}
	return false
}

func HasTraceParentHint(hints []*ast.TableOptimizerHint) (bool, string) {
	for _, hint := range hints {
		if strings.EqualFold(hint.HintName.String(), TraceParentHint) {
			hintData := hint.HintData.(model.CIStr)
			traceParent := hintData.String()
			return true, traceParent
		}
	}
	return false, ""
}

func NewXIDHint(xid string) *ast.TableOptimizerHint {
	return &ast.TableOptimizerHint{
		HintName: model.CIStr{
			O: XIDHint,
			L: strings.ToLower(XIDHint),
		},
		HintData: model.CIStr{
			O: xid,
			L: strings.ToLower(xid),
		},
	}
}
