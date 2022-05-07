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

package topo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_ParseTopology(t *testing.T) {
	tp, err := ParseTopology("school", "student", map[int]string{
		0: "0-9",
		1: "10-19",
		2: "20-29",
		3: "30-39",
		4: "40-49",
		5: "50-59",
		6: "60-69",
		7: "70-79",
		8: "80-89",
		9: "90-99",
	})

	assert.Nil(t, err)
	assert.Equal(t, 100, tp.TableSliceLen)
}
