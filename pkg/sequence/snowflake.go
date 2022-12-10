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

package sequence

import (
	"fmt"
	"math/rand"
	"net"
	"sync/atomic"

	"github.com/cectc/dbpack/pkg/misc"
)

const (
	// Start time cut (2020-05-03)
	epoch uint64 = 1588435200000

	// The number of bits occupied by workerID
	workerIDBits = 10

	// The number of bits occupied by timestamp
	timestampBits = 41

	// The number of bits occupied by sequence
	sequenceBits = 12

	// Maximum supported machine id, the result is 1023
	maxWorkerID = -1 ^ (-1 << workerIDBits)

	// mask that help to extract timestamp and sequence from a long
	timestampAndSequenceMask uint64 = -1 ^ (-1 << (timestampBits + sequenceBits))
)

type SnowflakeWorker struct {
	workerID             int64
	timestampAndSequence uint64
}

func NewWorker(id int64) (*SnowflakeWorker, error) {
	if id < 0 || id > maxWorkerID {
		return nil, fmt.Errorf("worker id can't be greater than %d or less than 0", maxWorkerID)
	}
	if id == 0 {
		id = generateWorkerID()
	}
	timestamp := getNewestTimestamp()
	timestampWithSequence := timestamp << sequenceBits
	return &SnowflakeWorker{
		workerID:             id << (timestampBits + sequenceBits),
		timestampAndSequence: timestampWithSequence,
	}, nil
}

func (w *SnowflakeWorker) NextID() (int64, error) {
	next := atomic.AddUint64(&w.timestampAndSequence, 1)
	timestampWithSequence := next & timestampAndSequenceMask

	return int64(uint64(w.workerID) | timestampWithSequence), nil
}

// get newest timestamp relative to twepoch
func getNewestTimestamp() uint64 {
	return misc.CurrentTimeMillis() - epoch
}

// auto generate workerID, try using mac first, if failed, then randomly generate one
func generateWorkerID() int64 {
	id, err := generateWorkerIDBaseOnMac()
	if err != nil {
		id = generateRandomWorkerID()
	}
	return id
}

// use lowest 10 bit of available MAC as workerID
func generateWorkerIDBaseOnMac() (int64, error) {
	ifaces, _ := net.Interfaces()
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue // interface down
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue // loopback interface
		}

		mac := iface.HardwareAddr

		return int64(int(rune(mac[4]&0b11)<<8) | int(mac[5]&0xFF)), nil
	}
	return 0, fmt.Errorf("no available mac found")
}

// randomly generate one as workerID
func generateRandomWorkerID() int64 {
	return rand.Int63n(maxWorkerID + 1)
}
