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
	"sync/atomic"
)

type SnowflakeWorker struct {
	workerID             int64
	timestampAndSequence uint64
}

func NewWorker(id int) (*SnowflakeWorker, error) {
	if id < 0 || id > maxWorkerID {
		return nil, fmt.Errorf("worker id can't be greater than %d or less than 0", maxWorkerID)
	}
	if id == 0 {
		workerID = generateWorkerID()
	}
	timestamp := getNewestTimestamp()
	timestampWithSequence := timestamp << sequenceBits
	atomic.StoreUint64(&timestampAndSequence, timestampWithSequence)
	return &SnowflakeWorker{
		workerID:             int64(id),
		timestampAndSequence: timestampAndSequence,
	}, nil
}

func (w *SnowflakeWorker) NextID() (int64, error) {
	next := atomic.AddUint64(&w.timestampAndSequence, 1)
	timestampWithSequence := next & timestampAndSequenceMask

	return int64(uint64(workerID) | timestampWithSequence), nil
}
