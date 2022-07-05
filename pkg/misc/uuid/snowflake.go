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

package uuid

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/cectc/dbpack/pkg/misc"
)

const (
	seqMask int32 = -1 ^ (-1 << sequenceBits)
)

type SnowflakeWorker struct {
	workerID int64
	lastTs   uint64
	seq      int32
	rnd      *rand.Rand
}

func NewWorker() (*SnowflakeWorker, error) {
	workerID := generateWorkerID()
	if workerID < 0 || workerID > maxWorkerID {
		return nil, fmt.Errorf("worker id can't be greater than %d or less than 0", maxWorkerID)
	}
	nano := time.Now().UTC().UnixNano()
	rnd := rand.New(rand.NewSource(nano))
	return &SnowflakeWorker{
		workerID: workerID,
		lastTs:   0,
		seq:      0,
		rnd:      rnd,
	}, nil
}

func (w *SnowflakeWorker) NextID() (int64, error) {
	ts := misc.CurrentTimeMillis()
	if ts < w.lastTs {
		offset := w.lastTs - ts
		if offset > 5 {
			return int64(0), fmt.Errorf("worker %d clock move back", w.workerID)
		}
		time.Sleep(time.Duration(offset<<1) * time.Millisecond)
		ts = misc.CurrentTimeMillis()
		if ts < w.lastTs {
			return int64(0), fmt.Errorf("worker %d clock move back", w.workerID)
		}
	}
	if ts == w.lastTs {
		w.seq = (w.seq + 1) & seqMask
		if w.seq == 0 {
			ts = w.waitUntilNextTs()
		}
	} else {
		w.seq = int32(w.rnd.Intn(10))
	}
	return w.makeID(ts)
}

func (w *SnowflakeWorker) makeID(now uint64) (int64, error) {
	w.lastTs = now
	timestamp := (now - epoch) << (sequenceBits + workerIDBits)
	workerID := w.workerID << sequenceBits
	id := int64(timestamp) | workerID | int64(w.seq)
	return int64(id), nil
}

func (w *SnowflakeWorker) waitUntilNextTs() uint64 {
	t := misc.CurrentTimeMillis()
	for t <= w.lastTs {
		time.Sleep(100 * time.Microsecond)
		t = misc.CurrentTimeMillis()
	}
	return t
}
