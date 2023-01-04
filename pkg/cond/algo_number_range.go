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
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/pkg/errors"

	"github.com/cectc/dbpack/pkg/misc"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/pkg/topo"
	"github.com/cectc/dbpack/third_party/parser/opcode"
)

const (
	rangeRegex = `^([\dKkMm]+)-([\dKkMm]+)$`
)

var (
	rangeRegexp = regexp.MustCompile(rangeRegex)
)

type Range struct {
	From int64
	To   int64
}

type NumberRange struct {
	shardingKey   string
	allowFullScan bool
	topology      *topo.Topology
	ranges        map[int]*Range
	idGenerator   proto.SequenceGenerator
}

func NewNumberRange(shardingKey string,
	allowFullScan bool,
	topology *topo.Topology,
	config map[string]interface{},
	generator proto.SequenceGenerator) (*NumberRange, error) {
	ranges, err := parseNumberRangeConfig(config)
	if err != nil {
		return nil, err
	}
	return &NumberRange{
		shardingKey:   shardingKey,
		allowFullScan: allowFullScan,
		topology:      topology,
		ranges:        ranges,
		idGenerator:   generator,
	}, nil
}

func (shard *NumberRange) HasShardingKey(key string) bool {
	conditionKey := misc.ParseColumn(key)
	return strings.EqualFold(shard.shardingKey, conditionKey)
}

func (shard *NumberRange) Shard(condition *KeyCondition) (Condition, error) {
	conditionKey := misc.ParseColumn(condition.Key)
	if !strings.EqualFold(shard.shardingKey, conditionKey) {
		return TrueCondition{}, nil
	}
	val, err := strconv.ParseInt(fmt.Sprintf("%v", condition.Value), 10, 64)
	if err != nil {
		return nil, err
	}
	var result []int
	switch condition.Op {
	case opcode.EQ:
		for idx, numberRange := range shard.ranges {
			if val >= numberRange.From && val < numberRange.To {
				result = append(result, idx)
			}
		}
	case opcode.NE:
		return TrueCondition{}, nil
	case opcode.LT:
		for idx, numberRange := range shard.ranges {
			if numberRange.From < val {
				result = append(result, idx)
			}
		}
	case opcode.LE:
		for idx, numberRange := range shard.ranges {
			if numberRange.From <= val {
				result = append(result, idx)
			}
		}
	case opcode.GT:
		for idx, numberRange := range shard.ranges {
			if numberRange.To > val {
				result = append(result, idx)
			}
		}
	case opcode.GE:
		for idx, numberRange := range shard.ranges {
			if numberRange.To >= val {
				result = append(result, idx)
			}
		}
	}
	return TableIndexSliceCondition(result), nil
}

func (shard *NumberRange) ShardRange(cond1, cond2 *KeyCondition) (Condition, error) {
	conditionKey := misc.ParseColumn(cond1.Key)
	if !strings.EqualFold(shard.shardingKey, conditionKey) {
		return TrueCondition{}, nil
	}
	val1, err := strconv.ParseInt(fmt.Sprintf("%v", cond1.Value), 10, 64)
	if err != nil {
		return nil, err
	}
	val2, err := strconv.ParseInt(fmt.Sprintf("%v", cond2.Value), 10, 64)
	if err != nil {
		return nil, err
	}
	var status = 0
	if cond1.Op == opcode.GT && cond2.Op == opcode.LT {
		status = 1
	}
	if cond1.Op == opcode.GT && cond2.Op == opcode.LE {
		status = 2
	}
	if cond1.Op == opcode.GE && cond2.Op == opcode.LT {
		status = 3
	}
	if cond1.Op == opcode.GE && cond2.Op == opcode.LE {
		status = 4
	}
	if cond1.Op == opcode.NE && cond2.Op == opcode.LT {
		status = 5
	}
	if cond1.Op == opcode.NE && cond2.Op == opcode.LE {
		status = 6
	}
	var result []int
	switch status {
	case 1:
		return shard.calculateRange(val1, val2), nil
	case 2:
		return shard.calculateRange(val1, val2), nil
	case 3:
		return shard.calculateRange(val1, val2), nil
	case 4:
		return shard.calculateRange(val1, val2), nil
	case 5:
		for idx, rg := range shard.ranges {
			if rg.From < val2 {
				result = append(result, idx)
			}
		}
	case 6:
		for idx, rg := range shard.ranges {
			if rg.From <= val2 {
				result = append(result, idx)
			}
		}
	}
	return TableIndexSliceCondition(result), nil
}

func (shard *NumberRange) AllShards() Condition {
	return TableIndexSliceCondition(shard.topology.TableSlice)
}

func (shard *NumberRange) AllowFullScan() bool {
	return shard.allowFullScan
}

func (shard *NumberRange) Topology() *topo.Topology {
	return shard.topology
}

func (shard *NumberRange) Equal(algorithm ShardingAlgorithm) bool {
	if algo, ok := algorithm.(*NumberRange); ok {
		if reflect.DeepEqual(shard.ranges, algo.ranges) &&
			shard.topology.Equal(algo.topology) {
			return true
		}
	}
	return false
}

func (shard *NumberRange) NextID() (int64, error) {
	if shard.idGenerator != nil {
		return shard.idGenerator.NextID()
	}
	return 0, errors.New("there is no sequence generator")
}

func (shard *NumberRange) calculateRange(begin, end int64) Condition {
	result := make([]int, 0)
	start := shard.calculate(begin)
	stop := shard.calculate(end)
	if start == -1 && stop == -1 {
		return FalseCondition{}
	}
	if stop >= start {
		for i := start; i <= stop; i++ {
			result = append(result, i)
		}
		return TableIndexSliceCondition(result)
	}
	return FalseCondition{}
}

func (shard *NumberRange) calculate(value int64) int {
	for idx, numberRange := range shard.ranges {
		if value >= numberRange.From && value <= numberRange.To {
			return idx
		}
	}
	return -1
}

func parseNumberRangeConfig(config map[string]interface{}) (map[int]*Range, error) {
	result := make(map[int]*Range)
	for key, value := range config {
		rangeString, ok := value.(string)
		if !ok {
			return nil, errors.New("incorrect number range format")
		}
		dbIdx, err := strconv.Atoi(key)
		if err != nil {
			return nil, err
		}
		numberRange, err := parseNumberRange(rangeString)
		if err != nil {
			return nil, err
		}
		result[dbIdx] = numberRange
	}
	return result, nil
}

func parseNumberRange(rangeString string) (*Range, error) {
	params := rangeRegexp.FindStringSubmatch(rangeString)
	if len(params) != 3 {
		return nil, errors.Errorf("incorrect number range format")
	}
	var (
		begin, end int64
		err        error
	)
	begin, err = parseNumber(params[1])
	if err != nil {
		return nil, err
	}

	end, err = parseNumber(params[2])
	if err != nil {
		return nil, err
	}
	if end <= begin {
		return nil, errors.Errorf("incorrect number range format")
	}
	return &Range{
		From: begin,
		To:   end,
	}, nil
}

func parseNumber(num string) (int64, error) {
	var (
		number int64
		err    error
	)
	if strings.HasSuffix(num, "k") || strings.HasSuffix(num, "K") {
		num = strings.TrimSuffix(num, "k")
		num = strings.TrimSuffix(num, "K")
		number, err = strconv.ParseInt(num, 10, 64)
		if err != nil {
			return 0, err
		}
		number = number * 1000
	} else if strings.HasSuffix(num, "m") || strings.HasSuffix(num, "M") {
		num = strings.TrimSuffix(num, "m")
		num = strings.TrimSuffix(num, "M")
		number, err = strconv.ParseInt(num, 10, 64)
		if err != nil {
			return 0, err
		}
		number = number * 10000
	} else {
		number, err = strconv.ParseInt(num, 10, 64)
		if err != nil {
			return 0, err
		}
	}
	return number, nil
}
