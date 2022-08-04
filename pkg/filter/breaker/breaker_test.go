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
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestBreaker(t *testing.T) {
	filter := newFilter()
	err := filter.PostHandle(context.Background(), nil, nil)
	assert.Nil(t, err)

	err = errors.New("test error")
	// breaker open
	for i := 0; i < 5; i++ {
		postErr := filter.PostHandle(context.Background(), nil, err)
		assert.Equal(t, err, postErr)
	}
	assert.Equal(t, open, filter.state)

	// breaker half open
	time.Sleep(10 * time.Second)
	assert.Equal(t, halfOpen, filter.state)

	// breaker open
	postErr := filter.PostHandle(context.Background(), nil, err)
	assert.Equal(t, err, postErr)
	assert.Equal(t, open, filter.state)

	// breaker half open
	time.Sleep(10 * time.Second)
	assert.Equal(t, halfOpen, filter.state)

	// breaker close
	for i := 0; i < 5; i++ {
		postErr := filter.PostHandle(context.Background(), nil, nil)
		assert.Nil(t, postErr)
	}
	assert.Equal(t, closed, filter.state)
}

func newFilter() *_filter {
	return &_filter{
		errorThreshold:   5,
		successThreshold: 5,
		timeout:          5 * time.Second,
	}
}
