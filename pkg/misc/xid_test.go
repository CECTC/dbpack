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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetTransactionID(t *testing.T) {
	testCases := []struct {
		in     string
		expect int64
	}{
		{
			in:     "gs/aggregationSvc/2612341069705662465",
			expect: 2612341069705662465,
		},
		{
			in:     "gs/aggregationSvc/0",
			expect: 0,
		},
	}
	for _, c := range testCases {
		t.Run(c.in, func(t *testing.T) {
			transactionID := GetTransactionID(c.in)
			assert.Equal(t, c.expect, transactionID)
		})
	}
}
