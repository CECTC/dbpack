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

package config

import (
	"bytes"
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/cectc/dbpack/pkg/lb"
)

type (
	// DataSourceRole ...
	DataSourceRole int

	// DataSourceType ...
	DataSourceType int

	ExecuteMode byte

	// DataSource ...
	DataSource struct {
		Name                     string        `yaml:"name" json:"name"`
		DSN                      string        `yaml:"dsn" json:"dsn"`
		Capacity                 int           `yaml:"capacity" json:"capacity"`         // connection pool capacity
		MaxCapacity              int           `yaml:"max_capacity" json:"max_capacity"` // max connection pool capacity
		IdleTimeout              time.Duration `yaml:"idle_timeout" json:"idle_timeout"` // close backend direct connection after idle_timeout,unit: seconds
		PingInterval             time.Duration `yaml:"ping_interval" json:"ping_interval"`
		PingTimesForChangeStatus int           `yaml:"ping_times_for_change_status" json:"ping_times_for_change_status"`
		Filters                  []string      `yaml:"filters" json:"filters"`
	}

	DataSourceRef struct {
		Name   string `yaml:"name" json:"name"`
		Weight string `yaml:"weight,omitempty" json:"weight,omitempty"`
	}

	ReadWriteSplittingConfig struct {
		LoadBalanceAlgorithm lb.LoadBalanceAlgorithm `yaml:"load_balance_algorithm" json:"load_balance_algorithm"`
		DataSources          []*DataSourceRef        `yaml:"data_sources" json:"data_sources"`
	}

	DataSourceRefGroup struct {
		Name        string                  `yaml:"name" json:"name"`
		LBAlgorithm lb.LoadBalanceAlgorithm `yaml:"load_balance_algorithm" json:"load_balance_algorithm"`
		DataSources []*DataSourceRef        `yaml:"data_sources" json:"data_sources"`
	}

	ShardingRule struct {
		Column            string     `yaml:"column" json:"column"`
		ShardingAlgorithm string     `yaml:"sharding_algorithm" json:"sharding_algorithm"`
		Config            Parameters `yaml:"config,omitempty" json:"config,omitempty"`
	}

	LogicTable struct {
		DBName        string         `yaml:"db_name" json:"db_name"`
		TableName     string         `yaml:"table_name" json:"table_name"`
		AllowFullScan bool           `yaml:"allow_full_scan" json:"allow_full_scan"`
		ShardingRule  *ShardingRule  `yaml:"sharding_rule" json:"sharding_rule"`
		Topology      map[int]string `yaml:"topology" json:"topology"`
	}

	ShardingConfig struct {
		DBGroups           []*DataSourceRefGroup `yaml:"db_groups" json:"db_groups"`
		LogicTables        []*LogicTable         `yaml:"logic_tables" json:"logic_tables"`
		TransactionTimeout int32                 `yaml:"transaction_timeout" json:"transaction_timeout"`
	}
)

const (
	Master DataSourceRole = iota
	Slave
	Meta
)

const (
	DBMysql DataSourceType = iota
	DBPostgresSql
)

const (
	SDB ExecuteMode = iota
	RWS
	SHD
)

func (r *DataSourceRole) UnmarshalText(text []byte) error {
	if r == nil {
		return errors.New("can't unmarshal a nil *DataSourceRole")
	}
	if !r.unmarshalText(bytes.ToLower(text)) {
		return fmt.Errorf("unrecognized data source role: %q", text)
	}
	return nil
}

func (r *DataSourceRole) unmarshalText(text []byte) bool {
	switch string(text) {
	case "master":
		*r = Master
	case "slave":
		*r = Slave
	case "meta":
		*r = Meta
	default:
		return false
	}
	return true
}

func (t *DataSourceType) UnmarshalText(text []byte) error {
	if t == nil {
		return errors.New("can't unmarshal a nil *DataSourceType")
	}
	if !t.unmarshalText(bytes.ToLower(text)) {
		return fmt.Errorf("unrecognized data srouce type: %q", text)
	}
	return nil
}

func (t *DataSourceType) unmarshalText(text []byte) bool {
	switch string(text) {
	case "mysql":
		*t = DBMysql
	case "postgresql":
		*t = DBPostgresSql
	default:
		return false
	}
	return true
}

func (m ExecuteMode) String() string {
	switch m {
	case SDB:
		return "SDB"
	case RWS:
		return "RWS"
	case SHD:
		return "SHD"
	default:
		return fmt.Sprintf("%d", m)
	}
}

func (m *ExecuteMode) UnmarshalText(text []byte) error {
	if m == nil {
		return errors.New("can't unmarshal a nil *ExecuteMode")
	}
	if !m.unmarshalText(bytes.ToLower(text)) {
		return fmt.Errorf("unrecognized execute mode: %q", text)
	}
	return nil
}

func (m *ExecuteMode) unmarshalText(text []byte) bool {
	switch string(text) {
	case "sdb":
		*m = SDB
	case "rws":
		*m = RWS
	case "shd":
		*m = SHD
	default:
		return false
	}
	return true
}
