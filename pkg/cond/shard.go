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
	"sort"

	"github.com/cectc/dbpack/pkg/misc"
	"github.com/cectc/dbpack/pkg/topo"
	"github.com/cectc/dbpack/third_party/parser/opcode"
)

type TableIndexSliceCondition []int

func (cond TableIndexSliceCondition) And(cond2 Condition) Condition {
	switch c := cond2.(type) {
	case TrueCondition, FalseCondition:
		return c.And(cond)
	case TableIndexSliceCondition:
		m := make(map[int]int)
		result := make([]int, 0)
		for _, v := range cond {
			m[v]++
		}
		for _, v := range c {
			times, _ := m[v]
			if times > 0 {
				result = append(result, v)
			}
		}
		return TableIndexSliceCondition(result)
	}
	return nil
}

func (cond TableIndexSliceCondition) Or(cond2 Condition) Condition {
	switch c := cond2.(type) {
	case TrueCondition, FalseCondition:
		return c.Or(cond)
	case TableIndexSliceCondition:
		m := make(map[int]int)
		result := make([]int, 0)
		for _, v := range cond {
			m[v]++
			result = append(result, v)
		}
		for _, v := range c {
			times, _ := m[v]
			if times == 0 {
				result = append(result, v)
			}
		}
		return TableIndexSliceCondition(result)
	}
	return nil
}

// ParseTopology return whether you need full scan, and db tables map
func (cond TableIndexSliceCondition) ParseTopology(topology *topo.Topology) (bool, map[string][]string) {
	result := make(map[string][]string, 0)
	for _, index := range cond {
		table := topology.TableIndexMap[index]
		db := topology.Tables[table]
		result[db] = append(result[db], table)
	}
	return len(cond) == topology.TableSliceLen, result
}

type ConditionShard interface {
	Shard(alg ShardingAlgorithm) (TableIndexSliceCondition, error)
}

func (cond *ComplexCondition) Shard(alg ShardingAlgorithm) (TableIndexSliceCondition, error) {
	var (
		keyGroup   []Condition
		trueGroup  []Condition
		falseGroup []Condition
		result     = make([]Condition, 0, len(cond.Conditions))
	)

	for _, cd := range cond.Conditions {
		switch c := cd.(type) {
		case *ComplexCondition:
			resultCondition, err := c.Shard(alg)
			if err != nil {
				return nil, err
			}
			result = append(result, resultCondition)
		case *KeyCondition:
			keyGroup = append(keyGroup, c)
		case TrueCondition:
			trueGroup = append(trueGroup, c)
		case FalseCondition:
			falseGroup = append(falseGroup, c)
		}
	}
	if cond.Op == opcode.And && len(falseGroup) > 0 {
		return FalseCondition{}.Shard(alg)
	}
	if cond.Op == opcode.Or && len(trueGroup) > 0 {
		return TrueCondition{}.Shard(alg)
	}
	sort.Sort(ConditionSlice(keyGroup))

	if len(keyGroup) == 1 {
		sliceCondition, err := keyGroup[0].(*KeyCondition).Shard(alg)
		if err != nil {
			return nil, err
		}
		result = append(result, sliceCondition)
	}
	if len(keyGroup) > 1 {
		var err error
		switch cond.Op {
		case opcode.And:
			for i := 0; ; {
				c1 := keyGroup[i].(*KeyCondition)
				c2 := keyGroup[i+1].(*KeyCondition)
				if c1.Key != c2.Key {
					rlt, err := c1.Shard(alg)
					if err != nil {
						return nil, err
					}
					result = append(result, rlt)
					i++
				}
				if c1.Key == c2.Key {
					rlt, err := And(alg, c1, c2)
					if err != nil {
						return nil, err
					}
					result = append(result, rlt)
					i = i + 2
				}
				if i == len(keyGroup) {
					break
				}
				if i == len(keyGroup)-1 {
					ci := keyGroup[i].(*KeyCondition)
					rlt, err := ci.Shard(alg)
					if err != nil {
						return nil, err
					}
					result = append(result, rlt)
					break
				}
			}
		case opcode.Or:
			c := keyGroup[0]
			for i := 1; i < len(keyGroup); i++ {
				c, err = Or(alg, c, keyGroup[i])
				if err != nil {
					return nil, err
				}
			}
			result = append(result, c)
		}
	}

	c := result[0]
	if cond.Op == opcode.And {
		for i := 1; i < len(result); i++ {
			c = c.And(result[i])
		}
	}
	if cond.Op == opcode.Or {
		for i := 1; i < len(result); i++ {
			c = c.Or(result[i])
		}
	}
	switch c.(type) {
	case TrueCondition:
		return alg.AllShards().(TableIndexSliceCondition), nil
	case FalseCondition:
		return nil, nil
	}
	return c.(TableIndexSliceCondition), nil
}

func (cond *KeyCondition) Shard(alg ShardingAlgorithm) (TableIndexSliceCondition, error) {
	c, err := alg.Shard(cond)
	if err != nil {
		return nil, err
	}
	if slice, ok := c.(TableIndexSliceCondition); ok {
		return slice, nil
	}
	return c.(ConditionShard).Shard(alg)
}

func (cond TrueCondition) Shard(alg ShardingAlgorithm) (TableIndexSliceCondition, error) {
	return alg.AllShards().(TableIndexSliceCondition), nil
}

func (cond FalseCondition) Shard(alg ShardingAlgorithm) (TableIndexSliceCondition, error) {
	return nil, nil
}

// And
// condition1, condition2 TrueCondition FalseCondition KeyCondition
// return TrueCondition FalseCondition TableIndexSliceCondition
func And(alg ShardingAlgorithm, k1, k2 *KeyCondition) (Condition, error) {
	switch k1.Op {
	case opcode.EQ:
		switch k2.Op {
		case opcode.EQ:
			// a = 1 and a = 1 => a = 1
			if k1.Value == k2.Value {
				return k1.Shard(alg)
			}
			// a = 1 and a = 2 => always false
			return FalseCondition{}, nil
		case opcode.NE:
			// a = 1 and a <> 1 => always false
			if k1.Value == k2.Value {
				return FalseCondition{}, nil
			}
			// a = 1 and a <> 2 => a = 1
			return k1.Shard(alg)
		case opcode.LT:
			switch misc.Compare(k2.Value, k1.Value) {
			case -1, 0:
				// a = 1 and a < 1 => always false
				return FalseCondition{}, nil
			default:
				// a = 1 and a < 2 => a = 1
				return k1.Shard(alg)
			}
		case opcode.LE:
			switch misc.Compare(k2.Value, k1.Value) {
			case -1:
				// a = 2 and a <= 1 => always false
				return FalseCondition{}, nil
			default:
				// a = 2 and a <= 2 => a = 2
				return k1.Shard(alg)
			}
		case opcode.GT:
			switch misc.Compare(k2.Value, k1.Value) {
			case 1, 0:
				// a = 1 and a > 1 => always false
				return FalseCondition{}, nil
			default:
				// a = 1 and a > 0 => a = 1
				return k1.Shard(alg)
			}
		case opcode.GE:
			switch misc.Compare(k2.Value, k1.Value) {
			case 1:
				// a = 1 and a >= 2 => always false
				return FalseCondition{}, nil
			default:
				// a = 2 and a >= 2 => a = 2
				return k1.Shard(alg)
			}
		}
	case opcode.NE:
		switch k2.Op {
		case opcode.EQ:
			if k1.Value == k2.Value {
				// a <> 1 and a = 1 -> always false
				return FalseCondition{}, nil
			}
			// a <> 1 and a = 2 -> a = 2
			return k2.Shard(alg)
		}
	case opcode.LT:
		switch k2.Op {
		case opcode.NE:
			switch misc.Compare(k2.Value, k1.Value) {
			case 1, 0:
				// a < 1 and a <>1 => a < 1
				return k1.Shard(alg)
			default:
				// a < 2 and a <> 1 => a < 2 and a <> 1
				return alg.ShardRange(k1, k2)
			}
		case opcode.EQ:
			switch misc.Compare(k2.Value, k1.Value) {
			case 1, 0:
				// a < 1 AND a = 1 => always false
				return FalseCondition{}, nil
			default:
				// a < 3 AND a = 1 => a = 1
				return k2.Shard(alg)
			}
		case opcode.GT:
			switch misc.Compare(k2.Value, k1.Value) {
			case 1, 0:
				// a < 1 AND a > 1 => always false
				return FalseCondition{}, nil
			default:
				return alg.ShardRange(k2, k1)
			}
		case opcode.GE:
			switch misc.Compare(k2.Value, k1.Value) {
			case 1, 0:
				// a < 1 AND a >= 2 => always false
				// a < 1 AND a >= 1 => always false
				return FalseCondition{}, nil
			default:
				return alg.ShardRange(k2, k1)
			}
		case opcode.LT:
			switch misc.Compare(k2.Value, k1.Value) {
			case -1:
				// a < 2 AND a < 1 => a < 1
				return k2.Shard(alg)
			default:
				// a < 1 AND a < 2 => a < 1
				// a < 1 AND a < 1 => a < 1
				return k1.Shard(alg)
			}
		case opcode.LE:
			switch misc.Compare(k2.Value, k1.Value) {
			case 1, 0:
				// a < 1 AND a <= 2 => a < 1
				// a < 1 AND a <= 1 => a < 1
				return k1.Shard(alg)
			default:
				// a < 2 AND a <= 1 => a <= 1
				return k2.Shard(alg)
			}
		}
	case opcode.LE:
		switch k2.Op {
		case opcode.EQ:
			switch misc.Compare(k2.Value, k1.Value) {
			case 1:
				// a <= 1 AND a = 2 => always false
				return FalseCondition{}, nil
			default:
				return k2.Shard(alg)
			}
		case opcode.NE:
			switch misc.Compare(k2.Value, k1.Value) {
			case 1:
				// a <= 1 and a <> 2 => a <= 1
				return k1.Shard(alg)
			case 0:
				// a <= 1 and a <> 1 => a < 1
				return (&KeyCondition{
					Key:   k1.Key,
					Op:    opcode.LT,
					Value: k1.Value,
				}).Shard(alg)
			default:
				// a <= 2 and a <> 1 => a <= 2 and a <> 1
				return alg.ShardRange(k1, k2)
			}
		case opcode.LT:
			switch misc.Compare(k2.Value, k1.Value) {
			case 1:
				// a <= 1 and a < 2 => a <= 1
				return k1.Shard(alg)
			default:
				// a <= 3 and a < 3 => a < 3
				// a <= 3 and a < 1 => a < 1
				return k2.Shard(alg)
			}
		case opcode.LE:
			switch misc.Compare(k2.Value, k1.Value) {
			case -1:
				// a <= 2 and a <= 1 => a <= 1
				return k2.Shard(alg)
			default:
				// a <= 2 and a <= 3 => a <= 2
				// a <= 2 and a <= 2 => a <= 2
				return k1.Shard(alg)
			}
		case opcode.GT:
			switch misc.Compare(k2.Value, k1.Value) {
			case 1, 0:
				// a <= 1 and a > 1 => always false
				return FalseCondition{}, nil
			default:
				return alg.ShardRange(k2, k1)
			}
		case opcode.GE:
			switch misc.Compare(k2.Value, k1.Value) {
			case 1:
				// a <= 1 and a >= 2 => always false
				return FalseCondition{}, nil
			case 0:
				// a <= 1 and a >= 1 => a = 1
				return (&KeyCondition{
					Key:   k1.Key,
					Op:    opcode.EQ,
					Value: k1.Value,
				}).Shard(alg)
			default:
				return alg.ShardRange(k2, k1)
			}
		}
	case opcode.GT:
		switch k2.Op {
		case opcode.EQ:
			switch misc.Compare(k2.Value, k1.Value) {
			case 1:
				// a > 1 and a = 2 => a = 2
				return k2.Shard(alg)
			default:
				// a > 2 and a = 2 => always false
				// a > 2 and a = 1 => always false
				return FalseCondition{}, nil
			}
		case opcode.NE:
			return TrueCondition{}, nil
		case opcode.GT:
			switch misc.Compare(k2.Value, k1.Value) {
			case 1:
				// a > 3 and a > 4 => a > 4
				return k2.Shard(alg)
			default:
				// a > 3 and a > 2 => a > 3
				// a > 3 and a > 3 => a > 3
				return k1.Shard(alg)
			}
		case opcode.GE:
			switch misc.Compare(k2.Value, k1.Value) {
			case 1:
				// a > 1 and a >= 2 => a >= 2
				return k2.Shard(alg)
			default:
				// a > 1 and a >= 1 => a > 1
				// a > 3 and a >= 2 => a > 3
				return k1.Shard(alg)
			}
		case opcode.LT:
			switch misc.Compare(k2.Value, k1.Value) {
			case -1, 0:
				// a > 2 and a < 1 => always false
				// a > 2 and a < 2 => always false
				return FalseCondition{}, nil
			default:
				return alg.ShardRange(k1, k2)
			}
		case opcode.LE:
			switch misc.Compare(k2.Value, k1.Value) {
			case -1, 0:
				// a > 2 and b <= 1 => always false
				// a > 2 and b <= 2 => always false
				return FalseCondition{}, nil
			default:
				return alg.ShardRange(k1, k2)
			}
		}
	case opcode.GE:
		switch k2.Op {
		case opcode.EQ:
			switch misc.Compare(k2.Value, k1.Value) {
			case 1, 0:
				// a >= 1 and a = 2 => a = 2
				// a >= 1 and a = 1 => a = 1
				return k2.Shard(alg)
			default:
				// a >= 2 and a = 1 => always false
				return FalseCondition{}, nil
			}
		case opcode.NE:
			return TrueCondition{}, nil
		case opcode.GT:
			switch misc.Compare(k2.Value, k1.Value) {
			case 1, 0:
				// a >= 1 and a > 2 => a > 2
				// a >= 1 and a > 1 => a > 1
				return k2.Shard(alg)
			default:
				// a >= 2 and a > 1 => a >= 2
				return k1.Shard(alg)
			}
		case opcode.GE:
			switch misc.Compare(k2.Value, k1.Value) {
			case 1:
				// a >= 1 and a >= 2 => a >= 2
				return k2.Shard(alg)
			default:
				// a >= 2 and a >= 1 => a >= 2
				// a >= 2 and a >= 2 => a >= 2
				return k1.Shard(alg)
			}
		case opcode.LT:
			switch misc.Compare(k2.Value, k1.Value) {
			case -1, 0:
				// a >= 3 and a < 2 => always false
				// a >= 3 and a < 3 => always false
				return FalseCondition{}, nil
			default:
				return alg.ShardRange(k1, k2)
			}
		case opcode.LE:
			switch misc.Compare(k2.Value, k1.Value) {
			case -1:
				// a >= 3 AND a <= 2 => always false
				return FalseCondition{}, nil
			case 0:
			default:
				return alg.ShardRange(k1, k2)
			}
		}
	}
	return nil, nil
}

// Or
// condition1, condition2 TrueCondition FalseCondition KeyCondition
// return TrueCondition FalseCondition TableIndexSliceCondition
func Or(alg ShardingAlgorithm, condition1, condition2 Condition) (Condition, error) {
	k1, ok1 := condition1.(*KeyCondition)
	k2, ok2 := condition2.(*KeyCondition)
	if ok1 && !ok2 {
		cond1, err := k1.Shard(alg)
		if err != nil {
			return nil, err
		}
		return cond1.Or(condition2), nil
	}
	if !ok1 && ok2 {
		cond2, err := k2.Shard(alg)
		if err != nil {
			return nil, err
		}
		return k1.Or(cond2), nil
	}
	if ok1 && ok2 {
		cond1, err := k1.Shard(alg)
		if err != nil {
			return nil, err
		}
		cond2, err := k2.Shard(alg)
		if err != nil {
			return nil, err
		}
		return cond1.Or(cond2), nil
	}
	return nil, nil
}
