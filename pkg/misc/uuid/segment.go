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
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

const (
	MaxRetry = 3
)

type SegmentWorker struct {
	Key        string
	Step       int32
	CurrentPos int32                  // current buffer pos
	Buffer     []*Segment             // use two buffer, one buffer offers ID, another as preload
	UpdateTime time.Time              //  mark update time
	mutex      sync.Mutex             //
	IsPreload  bool                   // mark worker finished preload or not
	Waiting    map[string][]chan byte // hang
}

type Segment struct {
	Cursor int64 // current index
	Max    int64
	Min    int64
	InitOk bool
}

type SegmentRecord struct {
	ID         int64  `json:"id" form:"id"` // pk
	BizTag     string `json:"biz_tag" form:"biz_tag"`
	MaxID      int64  `json:"max_id" form:"max_id"`
	Step       int32  `json:"step" form:"step"`
	UpdateTime int64  `json:"update_time" form:"update_time"`
}

func NewSegmentWorker(sr *SegmentRecord) (*SegmentWorker, error) {
	return &SegmentWorker{
		Key:        sr.BizTag,
		Step:       sr.Step,
		CurrentPos: 0, // use first buffer
		Buffer:     make([]*Segment, 0),
		UpdateTime: time.Now(),
		Waiting:    make(map[string][]chan byte),
		IsPreload:  false,
	}, nil
}

func (w *SegmentWorker) NextID() (int64, error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	var id int64
	curBuffer := w.Buffer[w.CurrentPos]
	// check if current buffer has seq
	if w.HasSeq() {
		id = atomic.AddInt64(&w.Buffer[w.CurrentPos].Cursor, 1)
		w.UpdateTime = time.Now()
	}

	// if current buffer already use 50% ids，check another buffer if preload
	if curBuffer.Max-id < int64(0.5*float32(w.Step)) && len(w.Buffer) <= 1 && !w.IsPreload {
		w.IsPreload = true
		cancel, _ := context.WithTimeout(context.Background(), 3*time.Second)
		go w.PreloadBuffer(cancel)
	}

	// first buffer use out
	if id == curBuffer.Max {
		// checkout to another buffer
		if len(w.Buffer) > 1 && w.Buffer[w.CurrentPos+1].InitOk {
			w.Buffer = append(w.Buffer[:0], w.Buffer[1:]...)
		}
	}
	// return if id valid
	if w.HasID(id) {
		return id, nil
	}

	// wait goroutine
	waitChan := make(chan byte, 1)
	w.Waiting[w.Key] = append(w.Waiting[w.Key], waitChan)

	w.mutex.Unlock()

	timer := time.NewTimer(500 * time.Millisecond) // wait 500ms
	select {
	case <-waitChan:
	case <-timer.C:
	}

	w.mutex.Lock()
	// second buffer still unready
	if len(w.Buffer) <= 1 {
		return 0, errors.New("get id failed")
	}
	// switch buffer if second buffer ok
	w.Buffer = append(w.Buffer[:0], w.Buffer[1:]...)
	if w.HasSeq() {
		id = atomic.AddInt64(&w.Buffer[w.CurrentPos].Cursor, 1)
		w.UpdateTime = time.Now()
	}
	return id, nil
}

func (w *SegmentWorker) HasSeq() bool {
	if w.Buffer[w.CurrentPos].InitOk && w.Buffer[w.CurrentPos].Cursor < w.Buffer[w.CurrentPos].Max {
		return true
	}
	return false
}

func (w *SegmentWorker) PreloadBuffer(ctx context.Context) error {
	for i := 0; i < MaxRetry; i++ {

	}
	return nil
}

func (w *SegmentWorker) HasID(id int64) bool {
	return id != 0
}
