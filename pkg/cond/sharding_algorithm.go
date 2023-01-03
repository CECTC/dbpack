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
	"github.com/pkg/errors"

	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/pkg/topo"
)

type ShardingAlgorithm interface {
	HasShardingKey(key string) bool
	Shard(condition *KeyCondition) (Condition, error)
	ShardRange(cond1, cond2 *KeyCondition) (Condition, error)
	AllShards() Condition
	AllowFullScan() bool
	Topology() *topo.Topology
	Equal(algorithm ShardingAlgorithm) bool
	proto.SequenceGenerator
}

func NewShardingAlgorithm(algorithm, shardingKey string,
	allowFullScan bool, topology *topo.Topology, config map[string]interface{}, generator proto.SequenceGenerator) (ShardingAlgorithm, error) {
	switch algorithm {
	case "NumberMod":
		return NewNumberMod(shardingKey, allowFullScan, topology, generator), nil
	case "NumberRange":
		return NewNumberRange(shardingKey, allowFullScan, topology, config, generator)
	}
	return nil, errors.Errorf("unsupported sharding algorithm: %s", algorithm)
}
