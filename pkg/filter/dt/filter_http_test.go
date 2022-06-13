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

package dt

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMathTransactionInfo(t *testing.T) {
	testCases := []*struct {
		requestPath     string
		transactionInfo *TransactionInfo
		matched         bool
	}{
		{
			"/v1/order/create",
			&TransactionInfo{
				RequestPath: "/v1/order/create",
				Timeout:     60000,
				MatchType:   Exact,
			},
			true,
		},
		{
			"/v1/order/create",
			&TransactionInfo{
				RequestPath: "/v1/order/",
				Timeout:     60000,
				MatchType:   Prefix,
			},
			true,
		},
		{
			"/v1/order/create",
			&TransactionInfo{
				RequestPath: "/v1/order(/.*)?",
				Timeout:     60000,
				MatchType:   Regex,
			},
			true,
		},
		{
			"/v1/order/delete",
			&TransactionInfo{
				RequestPath: "/v1/order/create",
				Timeout:     60000,
				MatchType:   Exact,
			},
			false,
		},
		{
			"/v1/so/create",
			&TransactionInfo{
				RequestPath: "/v1/order/",
				Timeout:     60000,
				MatchType:   Prefix,
			},
			false,
		},
		{
			"/v1/so/create",
			&TransactionInfo{
				RequestPath: "/v1/order(/.*)?",
				Timeout:     60000,
				MatchType:   Regex,
			},
			false,
		},
	}
	for _, c := range testCases {
		t.Run(c.requestPath, func(t *testing.T) {
			var filter *_httpFilter
			switch c.transactionInfo.MatchType {
			case Exact:
				filter = &_httpFilter{transactionInfoMap: map[string]*TransactionInfo{
					c.transactionInfo.RequestPath: c.transactionInfo,
				}}
			case Prefix, Regex:
				filter = &_httpFilter{transactionInfos: []*TransactionInfo{c.transactionInfo}}
			}
			_, found := filter.matchTransactionInfo(c.requestPath)
			assert.Equal(t, c.matched, found)
		})
	}
}
