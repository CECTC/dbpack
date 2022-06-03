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
	"strconv"
	"strings"

	"github.com/golang-module/carbon"

	"github.com/cectc/dbpack/pkg/topo"
	"github.com/cectc/dbpack/third_party/parser/opcode"
)

type ShardingAlgorithm interface {
	HasShardingKey(key string) bool
	Shard(condition *KeyCondition) (Condition, error)
	ShardRange(cond1, cond2 *KeyCondition) (Condition, error)
	AllShards() Condition
	AllowFullScan() bool
}

type NumberMod struct {
	shardingKey   string
	allowFullScan bool
	topology      *topo.Topology
}

func NewNumberMod(shardingKey string, allowFullScan bool, topology *topo.Topology) *NumberMod {
	return &NumberMod{
		shardingKey:   shardingKey,
		allowFullScan: allowFullScan,
		topology:      topology,
	}
}

func (shard *NumberMod) HasShardingKey(key string) bool {
	return strings.EqualFold(shard.shardingKey, key)
}

func (shard *NumberMod) Shard(condition *KeyCondition) (Condition, error) {
	if !strings.EqualFold(shard.shardingKey, condition.Key) {
		return TrueCondition{}, nil
	}
	val, err := strconv.ParseInt(fmt.Sprintf("%v", condition.Value), 10, 64)
	if err != nil {
		return nil, err
	}
	var result []int
	switch condition.Op {
	case opcode.EQ:
		idx := int(val % int64(shard.topology.TableSliceLen))
		result = append(result, idx)
	case opcode.NE:
		idx := int(val % int64(shard.topology.TableSliceLen))
		for _, index := range shard.topology.TableSlice {
			if index != idx {
				result = append(result, index)
			}
		}
	case opcode.LT:
		if val < int64(shard.topology.TableSliceLen) {
			// 0 ~ val-1
			for i := val - 1; i >= 0; i-- {
				result = append(result, int(i))
			}
		} else {
			// all
			return TrueCondition{}, nil
		}
	case opcode.LE:
		if val < int64(shard.topology.TableSliceLen)-1 {
			// 0 ~ val
			for i := val; i >= 0; i-- {
				result = append(result, int(i))
			}
		} else {
			// all
			return TrueCondition{}, nil
		}
	case opcode.GT:
		// all
		return TrueCondition{}, nil
	case opcode.GE:
		// all
		return TrueCondition{}, nil
	}
	return TableIndexSliceCondition(result), nil
}

func (shard *NumberMod) ShardRange(cond1, cond2 *KeyCondition) (Condition, error) {
	if !strings.EqualFold(shard.shardingKey, cond1.Key) {
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
		if val2-val1-1 >= int64(shard.topology.TableSliceLen) {
			return TrueCondition{}, nil
		} else {
			for i := val1 + 1; i < val2; i++ {
				idx := int(i % int64(shard.topology.TableSliceLen))
				result = append(result, idx)
			}
		}
	case 2:
		if val2-val1 >= int64(shard.topology.TableSliceLen) {
			return TrueCondition{}, nil
		} else {
			for i := val1 + 1; i <= val2; i++ {
				idx := int(i % int64(shard.topology.TableSliceLen))
				result = append(result, idx)
			}
		}
	case 3:
		if val2-val1 >= int64(shard.topology.TableSliceLen) {
			return TrueCondition{}, nil
		} else {
			for i := val1; i < val2; i++ {
				idx := int(i % int64(shard.topology.TableSliceLen))
				result = append(result, idx)
			}
		}
	case 4:
		if val2-val1+1 >= int64(shard.topology.TableSliceLen) {
			return TrueCondition{}, nil
		} else {
			for i := val1; i <= val2; i++ {
				idx := int(i % int64(shard.topology.TableSliceLen))
				result = append(result, idx)
			}
		}
	case 5:
		if val1 >= int64(shard.topology.TableSliceLen)-1 {
			return TrueCondition{}, nil
		} else {
			if val2 > int64(shard.topology.TableSliceLen)+val1 {
				return TrueCondition{}, nil
			} else {
				var indexMap = make(map[int]int, 0)
				for i := 0; i < int(val2); i++ {
					if i != int(val1) {
						idx := i % shard.topology.TableSliceLen
						indexMap[idx]++
					}
				}
				for k := range indexMap {
					result = append(result, k)
				}
			}
		}
	case 6:
		if val1 >= int64(shard.topology.TableSliceLen)-1 {
			return TrueCondition{}, nil
		} else {
			if val2 >= int64(shard.topology.TableSliceLen)+val1 {
				return TrueCondition{}, nil
			} else {
				var indexMap = make(map[int]int, 0)
				for i := 0; i < int(val2); i++ {
					if i != int(val1) {
						idx := i % shard.topology.TableSliceLen
						indexMap[idx]++
					}
				}
				for k := range indexMap {
					result = append(result, k)
				}
			}
		}
	}

	return TableIndexSliceCondition(result), nil
}

func (shard *NumberMod) AllShards() Condition {
	return TableIndexSliceCondition(shard.topology.TableSlice)
}

func (shard *NumberMod) AllowFullScan() bool {
	return shard.allowFullScan
}

type MonthMod struct {
	ShardNum int
}

func (shard *MonthMod) Shard(value interface{}) (int, error) {
	dateTime := carbon.Parse(fmt.Sprintf("%v", value))
	if dateTime.IsInvalid() {
		return 0, dateTime.Error
	}
	month := dateTime.Month()
	return month % shard.ShardNum, nil
}

type YearMod struct {
	ShardNum int
}

func (shard *YearMod) Shard(value interface{}) (int, error) {
	dateTime := carbon.Parse(fmt.Sprintf("%v", value))
	if dateTime.IsInvalid() {
		return 0, dateTime.Error
	}
	year := dateTime.Year()
	return year % shard.ShardNum, nil
}
