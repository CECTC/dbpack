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

package lb

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/atomic"
)

var counter = atomic.NewInt64(0)

type LoadBalanceAlgorithm int32

const (
	Random LoadBalanceAlgorithm = iota
	RoundRobin
	RandomWeight
)

func (l *LoadBalanceAlgorithm) UnmarshalText(text []byte) error {
	if l == nil {
		return errors.New("can't unmarshal a nil *ProtocolType")
	}
	if !l.unmarshalText(bytes.ToLower(text)) {
		return fmt.Errorf("unrecognized protocol type: %s", text)
	}
	return nil
}

func (l *LoadBalanceAlgorithm) unmarshalText(text []byte) bool {
	alg := string(text)
	if strings.EqualFold(alg, "Random") {
		*l = Random
		return true
	}
	if strings.EqualFold(alg, "RoundRobin") {
		*l = RoundRobin
		return true
	}
	if strings.EqualFold(alg, "RandomWeight") {
		*l = RandomWeight
		return true
	}
	return false
}

type Weighting interface {
	Counting() bool
	Weight(ctx context.Context) int
}

type Interface interface {
	Add(Weighting)
	Next(ctx context.Context) Weighting
}

type RandomAlgorithm struct {
	items []Weighting
}

func (r *RandomAlgorithm) Add(item Weighting) {
	r.items = append(r.items, item)
}

func (r *RandomAlgorithm) Next(ctx context.Context) Weighting {
	var items []Weighting
	for _, item := range r.items {
		if item.Counting() {
			items = append(items, item)
		}
	}
	total := len(items)
	index := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(total)
	return items[index]
}

type RoundRobinAlgorithm struct {
	items []Weighting
}

func (r *RoundRobinAlgorithm) Add(item Weighting) {
	r.items = append(r.items, item)
}

func (r *RoundRobinAlgorithm) Next(ctx context.Context) Weighting {
	var items []Weighting
	for i := 0; i < len(r.items); i++ {
		item := r.items[i]
		if item.Counting() {
			items = append(items, item)
		}
	}
	total := len(items)
	counter.Add(1)
	count := counter.Load()
	index := count % int64(total)
	return items[index]
}

type RandomWeightAlgorithm struct {
	items []Weighting
}

func (r *RandomWeightAlgorithm) Add(item Weighting) {
	r.items = append(r.items, item)
}

func (r *RandomWeightAlgorithm) Next(ctx context.Context) Weighting {
	var (
		items       []Weighting
		totalWeight = 0
		weightSum   = 0
	)
	for i := 0; i < len(r.items); i++ {
		item := r.items[i]
		if item.Counting() {
			items = append(items, item)
			totalWeight += item.Weight(ctx)
		}
	}
	index := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(totalWeight)
	for i := 0; i < len(items); i++ {
		if index >= weightSum && index < weightSum+items[i].Weight(ctx) {
			return items[i]
		}
		weightSum += items[i].Weight(ctx)
	}
	return nil
}

func New(alg LoadBalanceAlgorithm) (Interface, error) {
	switch alg {
	case Random:
		return &RandomAlgorithm{}, nil
	case RoundRobin:
		return &RoundRobinAlgorithm{}, nil
	case RandomWeight:
		return &RandomWeightAlgorithm{}, nil
	default:
		return nil, errors.Errorf("unsupported load balance algorithm %d", alg)
	}
}
