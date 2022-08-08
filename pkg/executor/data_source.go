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

package executor

import (
	"context"
	"regexp"
	"strconv"

	"github.com/pkg/errors"

	"github.com/cectc/dbpack/pkg/config"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/pkg/resource"
)

const (
	weightRegex = `^r([\d]+)w([\d]+)$`
)

type DataSourceBrief struct {
	Name        string `yaml:"name" json:"name"`
	WriteWeight int    `yaml:"write_weight" json:"write_weight"`
	ReadWeight  int    `yaml:"read_weight" json:"read_weight"`
	IsMaster    bool   `yaml:"is_master" json:"is_master"`
	DB          proto.DB
}

func (brief *DataSourceBrief) Counting() bool {
	return brief.DB.Status() == proto.Running
}

func (brief *DataSourceBrief) Weight(ctx context.Context) int {
	if proto.IsMaster(ctx) {
		return brief.WriteWeight
	}
	return brief.ReadWeight
}

func castToDataSourceBrief(appid string, ref *config.DataSourceRef) (*DataSourceBrief, error) {
	weightRegexp := regexp.MustCompile(weightRegex)
	params := weightRegexp.FindStringSubmatch(ref.Weight)
	if len(params) != 3 {
		return nil, errors.Errorf("datasource reference '%s' weight invalid: %s", ref.Name, ref.Weight)
	}
	readWeight := params[1]
	writeWeight := params[2]
	rw, err := strconv.Atoi(readWeight)
	if err != nil {
		return nil, errors.Errorf("cast read weight for datasource reference '%s' failed, read weight: %s", ref.Name, readWeight)
	}
	ww, err := strconv.Atoi(writeWeight)
	if err != nil {
		return nil, errors.Errorf("cast write weight for datasource reference '%s' failed, write weight: %s", ref.Name, readWeight)
	}
	db := resource.GetDBManager(appid).GetDB(ref.Name)
	return &DataSourceBrief{
		Name:        ref.Name,
		WriteWeight: ww,
		ReadWeight:  rw,
		IsMaster:    ww > 0,
		DB:          db,
	}, nil
}

// groupDataSourceRefs cast DataSourceRef to DataSourceBrief, then group them.
func groupDataSourceRefs(appid string, dataSources []*config.DataSourceRef) (all, masters, reads []*DataSourceBrief, err error) {
	for i := 0; i < len(dataSources); i++ {
		ds := dataSources[i]
		brief, err := castToDataSourceBrief(appid, ds)
		if err != nil {
			return nil, nil, nil, err
		}
		if brief.IsMaster {
			masters = append(masters, brief)
		}
		if brief.ReadWeight > 0 {
			reads = append(reads, brief)
		}
		all = append(all, brief)
	}
	return
}
