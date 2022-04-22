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

	"github.com/cectc/dbpack/pkg/lb"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/pkg/resource"
)

type (
	DataSourceRef struct {
		Name   string `yaml:"name" json:"name"`
		Weight string `yaml:"weight,omitempty" json:"weight,omitempty"`
	}

	DataSourceBrief struct {
		Name        string `yaml:"name" json:"name"`
		WriteWeight int    `yaml:"write_weight" json:"write_weight"`
		ReadWeight  int    `yaml:"read_weight" json:"read_weight"`
		IsMaster    bool   `yaml:"is_master" json:"is_master"`
		DB          proto.DB
	}

	ReadWriteSplittingConfig struct {
		LoadBalanceAlgorithm lb.LoadBalanceAlgorithm `yaml:"load_balance_algorithm" json:"load_balance_algorithm"`
		DataSources          []*DataSourceRef        `yaml:"data_sources" json:"data_sources"`
	}

	DataSourceRefGroup struct {
		Name  string           `yaml:"name" json:"name"`
		Group []*DataSourceRef `yaml:"group" json:"group"`
	}

	ShardingRule struct {
		Column string `yaml:"column" json:"column"`
		Expr   string `yaml:"expr" json:"expr"`
	}

	VirtualTableRule struct {
		Name               string          `yaml:"name" json:"name"`
		AllowFullTableScan bool            `yaml:"allow_full_table_scan" json:"allow_full_table_scan"`
		ShardingDB         []*ShardingRule `yaml:"sharding_db" json:"sharding_db"`
		ShardingTable      []*ShardingRule `yaml:"sharding_table" json:"sharding_table"`
		Topology           *Topology       `yaml:"topology" json:"topology"`
		ShadowTopology     *Topology       `yaml:"shadow_topology" json:"shadow_topology"`
	}

	Topology struct {
		DBNamePattern    string `yaml:"db_name_pattern" json:"db_name_pattern"`
		TableNamePattern string `yaml:"table_name_pattern" json:"table_name_pattern"`
	}
)

func (brief *DataSourceBrief) Counting() bool {
	return brief.DB.Status() == proto.Running
}

func (brief *DataSourceBrief) Weight(ctx context.Context) int {
	if proto.IsMaster(ctx) {
		return brief.WriteWeight
	}
	return brief.ReadWeight
}

func (ref *DataSourceRef) castToDataSourceBrief() (*DataSourceBrief, error) {
	weightRegexp := regexp.MustCompile(`^r([\d]+)w([\d]+)$`)
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
	db := resource.GetDBManager().GetDB(ref.Name)
	return &DataSourceBrief{
		Name:        ref.Name,
		WriteWeight: ww,
		ReadWeight:  rw,
		IsMaster:    ww > 0,
		DB:          db,
	}, nil
}

// groupDataSourceRefs cast DataSourceRef to DataSourceBrief, then group them.
func groupDataSourceRefs(dataSources []*DataSourceRef) (masters []*DataSourceBrief, reads []*DataSourceBrief, err error) {
	for i := 0; i < len(dataSources); i++ {
		ds := dataSources[i]
		brief, err := ds.castToDataSourceBrief()
		if err != nil {
			return nil, nil, err
		}
		if brief.IsMaster {
			masters = append(masters, brief)
		}
		if brief.ReadWeight > 0 {
			reads = append(reads, brief)
		}
	}
	return
}
