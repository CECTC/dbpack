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

package cond

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseNumber(t *testing.T) {
	testcases := []struct {
		num          string
		expectNumber int64
	}{
		{
			num:          "1000",
			expectNumber: int64(1000),
		},
		{
			num:          "1000k",
			expectNumber: int64(1000000),
		},
		{
			num:          "1000K",
			expectNumber: int64(1000000),
		},
		{
			num:          "1000m",
			expectNumber: int64(10000000),
		},
		{
			num:          "1000M",
			expectNumber: int64(10000000),
		},
	}
	for _, tc := range testcases {
		t.Run(tc.num, func(t *testing.T) {
			number, err := parseNumber(tc.num)
			assert.Nil(t, err)
			assert.Equal(t, tc.expectNumber, number)
		})
	}
}

func TestParseNumberRange(t *testing.T) {
	testcases := []struct {
		rangeString string
		expectRange *Range
	}{
		{
			rangeString: "1000-2000",
			expectRange: &Range{
				From: 1000,
				To:   2000,
			},
		},
		{
			rangeString: "1000k-2000k",
			expectRange: &Range{
				From: 1000000,
				To:   2000000,
			},
		},
		{
			rangeString: "1000K-2000K",
			expectRange: &Range{
				From: 1000000,
				To:   2000000,
			},
		},
		{
			rangeString: "1000m-2000m",
			expectRange: &Range{
				From: 10000000,
				To:   20000000,
			},
		},
		{
			rangeString: "1000M-2000M",
			expectRange: &Range{
				From: 10000000,
				To:   20000000,
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.rangeString, func(t *testing.T) {
			numberRange, err := parseNumberRange(tc.rangeString)
			assert.Nil(t, err)
			assert.Equal(t, tc.expectRange, numberRange)
		})
	}
}

func TestParseNumberRangeConfig(t *testing.T) {
	testcases := []struct {
		caseName    string
		rangeConfig map[string]interface{}
		expectRange map[int]*Range
	}{
		{
			caseName: "case1",
			rangeConfig: map[string]interface{}{
				"0": "0-100k",
				"1": "100k-200k",
				"2": "200k-300k",
				"3": "300k-400k",
				"4": "400k-500k",
				"5": "500k-600k",
				"6": "600k-700k",
				"7": "700k-800k",
				"8": "800k-900k",
				"9": "900k-1000k",
			},
			expectRange: map[int]*Range{
				0: &Range{
					From: 0,
					To:   100000,
				},
				1: &Range{
					From: 100000,
					To:   200000,
				},
				2: &Range{
					From: 200000,
					To:   300000,
				},
				3: &Range{
					From: 300000,
					To:   400000,
				},
				4: &Range{
					From: 400000,
					To:   500000,
				},
				5: &Range{
					From: 500000,
					To:   600000,
				},
				6: &Range{
					From: 600000,
					To:   700000,
				},
				7: &Range{
					From: 700000,
					To:   800000,
				},
				8: &Range{
					From: 800000,
					To:   900000,
				},
				9: &Range{
					From: 900000,
					To:   1000000,
				},
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.caseName, func(t *testing.T) {
			ranges, err := parseNumberRangeConfig(tc.rangeConfig)
			assert.Nil(t, err)
			for k, v := range ranges {
				assert.Equal(t, tc.expectRange[k], v)
			}
		})
	}
}
