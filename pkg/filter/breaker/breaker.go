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

package breaker

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/cectc/dbpack/pkg/filter"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/proto"
)

const circuitBreakFilter = "CircuitBreakerFilter"

// ErrBreakerOpen is the error returned from PreHandle() when the function is not executed
// because the breaker is currently open.
var ErrBreakerOpen = errors.New("circuit breaker is open")

const (
	closed uint32 = iota
	open
	halfOpen
)

type _factory struct{}

func (factory *_factory) NewFilter(_ string, config map[string]interface{}) (proto.Filter, error) {
	var (
		err     error
		content []byte
		conf    *CircuitBreakerConfig
	)
	if content, err = json.Marshal(config); err != nil {
		return nil, errors.Wrap(err, "marshal circuit breaker filter config failed.")
	}
	if err = json.Unmarshal(content, &conf); err != nil {
		log.Errorf("unmarshal circuit breaker filter failed, %v", err)
		return nil, err
	}

	return &_filter{
		errorThreshold:   conf.ErrorThreshold,
		successThreshold: conf.SuccessThreshold,
		timeout:          time.Duration(conf.Timeout) * time.Second,
	}, nil
}

type _filter struct {
	errorThreshold   int
	successThreshold int
	timeout          time.Duration

	lock      sync.Mutex
	state     uint32
	errors    int
	successes int
	lastError time.Time
}

type CircuitBreakerConfig struct {
	ErrorThreshold   int
	SuccessThreshold int
	Timeout          int
}

func (f *_filter) GetKind() string {
	return circuitBreakFilter
}

func (f *_filter) PreHandle(ctx context.Context) error {
	state := atomic.LoadUint32(&f.state)

	if state == open {
		return ErrBreakerOpen
	}

	return nil
}

func (f *_filter) PostHandle(ctx context.Context, result proto.Result, err error) error {
	state := atomic.LoadUint32(&f.state)
	if err == nil && state == closed {
		return nil
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	if err == nil {
		if f.state == halfOpen {
			f.successes++
			if f.successes == f.successThreshold {
				f.closeBreaker()
			}
		}
	} else {
		if f.errors > 0 {
			expiry := f.lastError.Add(f.timeout)
			if time.Now().After(expiry) {
				f.errors = 0
			}
		}

		switch f.state {
		case closed:
			f.errors++
			if f.errors == f.errorThreshold {
				f.openBreaker()
			} else {
				f.lastError = time.Now()
			}
		case halfOpen:
			f.openBreaker()
		}
	}
	return err
}

func (f *_filter) openBreaker() {
	f.changeState(open)
	go f.timer()
}

func (f *_filter) closeBreaker() {
	f.changeState(closed)
}

func (f *_filter) timer() {
	time.Sleep(f.timeout)

	f.lock.Lock()
	defer f.lock.Unlock()

	f.changeState(halfOpen)
}

func (f *_filter) changeState(newState uint32) {
	f.errors = 0
	f.successes = 0
	atomic.StoreUint32(&f.state, newState)
}

func init() {
	filter.RegistryFilterFactory(circuitBreakFilter, &_factory{})
}
