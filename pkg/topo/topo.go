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
	"fmt"
	"regexp"
	"strconv"

	"github.com/cznic/mathutil"
	"github.com/pkg/errors"
)

const (
	topologyRegex = `^(\d+)-(\d+)$`
)

type Topology struct {
	DBName    string
	TableName string
	// dbName -> table slice
	DBs map[string][]string
	// tableName -> dbName
	Tables map[string]string
	// index -> tableName
	TableIndexMap map[int]string
	TableSlice    []int
	TableSliceLen int
}

func ParseTopology(dbName, tableName string, topology map[int]string) (*Topology, error) {
	var (
		dbLen           = len(topology)
		topologyRegexp  = regexp.MustCompile(topologyRegex)
		dbs             = make(map[string][]string, 0)
		tables          = make(map[string]string, 0)
		tableIndexMap   = make(map[int]string, 0)
		tableIndexSlice = make([]int, 0)
		max             = 0
	)

	for i := 0; i < dbLen; i++ {
		tp := topology[i]
		realDB := fmt.Sprintf("%s_%d", dbName, i)
		params := topologyRegexp.FindStringSubmatch(tp)
		if len(params) != 3 {
			return nil, errors.Errorf("incorrect topology format")
		}
		begin, err := strconv.Atoi(params[1])
		if err != nil {
			return nil, err
		}
		end, err := strconv.Atoi(params[2])
		if err != nil {
			return nil, err
		}
		if begin >= end {
			return nil, errors.Errorf("incorrect topology, begin index must less than end index")
		}
		tableSlice := make([]string, 0)
		for j := begin; j <= end; j++ {
			index := j
			realTable := fmt.Sprintf("%s_%d", tableName, index)
			tables[realTable] = realDB
			tableIndexMap[index] = realTable
			tableIndexSlice = append(tableIndexSlice, index)
			tableSlice = append(tableSlice, realTable)
			max = mathutil.Max(max, index)
		}
		dbs[realDB] = tableSlice
	}
	if max != len(tableIndexSlice)-1 {
		return nil, errors.Errorf("table index must from 0 to %d", len(tableIndexSlice)-1)
	}
	return &Topology{
		DBName:        dbName,
		TableName:     tableName,
		DBs:           dbs,
		Tables:        tables,
		TableIndexMap: tableIndexMap,
		TableSlice:    tableIndexSlice,
		TableSliceLen: len(tableIndexSlice),
	}, nil
}
