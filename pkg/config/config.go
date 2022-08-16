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
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"gopkg.in/yaml.v3"

	"github.com/cectc/dbpack/pkg/log"
)

// ProtocolType protocol type enum
type ProtocolType int32

const (
	Http ProtocolType = iota
	Mysql
)

func (t *ProtocolType) UnmarshalText(text []byte) error {
	if t == nil {
		return errors.New("can't unmarshal a nil *ProtocolType")
	}
	if !t.unmarshalText(bytes.ToLower(text)) {
		return fmt.Errorf("unrecognized protocol type: %q", text)
	}
	return nil
}

func (t *ProtocolType) unmarshalText(text []byte) bool {
	switch string(text) {
	case "mysql":
		*t = Mysql
	case "http":
		*t = Http
	default:
		return false
	}
	return true
}

type Configuration struct {
	ProbePort                int           `default:"18888" yaml:"probe_port" json:"probe_port"`
	Tracer                   *TracerConfig `yaml:"tracer" json:"tracer"`
	TerminationDrainDuration time.Duration `default:"3s" yaml:"termination_drain_duration" json:"termination_drain_duration"`

	AppConfig AppConfig `yaml:"app_config" json:"app_config"`
}

type AppConfig map[string]*DBPackConfig

type DBPackConfig struct {
	AppID                  string                  `yaml:"-" json:"-"`
	DistributedTransaction *DistributedTransaction `yaml:"distributed_transaction" json:"distributed_transaction"`

	Listeners   []*Listener   `yaml:"listeners" json:"listeners"`
	Executors   []*Executor   `yaml:"executors" json:"executors"`
	DataSources []*DataSource `yaml:"data_source_cluster" json:"data_source_cluster"`
	Filters     []*Filter     `yaml:"filters" json:"filters"`
}

type TracerConfig struct {
	ExporterType     string  `yaml:"exporter_type" json:"exporter_type"`
	ExporterEndpoint *string `yaml:"exporter_endpoint" json:"exporter_endpoint"`
}

type DistributedTransaction struct {
	AppID                            string `yaml:"appid" json:"appid"`
	RetryDeadThreshold               int64  `yaml:"retry_dead_threshold" json:"retry_dead_threshold"`
	RollbackRetryTimeoutUnlockEnable bool   `yaml:"rollback_retry_timeout_unlock_enable" json:"rollback_retry_timeout_unlock_enable"`

	EtcdConfig *clientv3.Config `yaml:"etcd_config" json:"etcd_config"`
}

type Listener struct {
	AppID         string        `yaml:"-" json:"-"`
	ProtocolType  ProtocolType  `yaml:"protocol_type" json:"protocol_type"`
	SocketAddress SocketAddress `yaml:"socket_address" json:"socket_address"`
	Config        Parameters    `yaml:"config" json:"config"`
	Executor      string        `yaml:"executor" json:"executor"`
	Filters       []string      `yaml:"filters" json:"filters"`
}

type Executor struct {
	AppID   string      `yaml:"-" json:"-"`
	Name    string      `yaml:"name" json:"name"`
	Mode    ExecuteMode `yaml:"mode" json:"mode"`
	Config  Parameters  `yaml:"config" json:"config"`
	Filters []string    `yaml:"filters" json:"filters"`
}

type Filter struct {
	AppID  string     `yaml:"-" json:"-"`
	Name   string     `yaml:"name" json:"name"`
	Kind   string     `yaml:"kind" json:"kind"`
	Config Parameters `yaml:"conf,omitempty" json:"conf,omitempty"`
}

// SocketAddress specify either a logical or physical address and port, which are
// used to tell server where to bind/listen, connect to upstream and find
// management servers
type SocketAddress struct {
	Address string `default:"0.0.0.0" yaml:"address" json:"address"`
	Port    int    `default:"8881" yaml:"port" json:"port"`
}

// Parameters defines a key-value parameters mapping
type Parameters map[string]interface{}

func (config Configuration) DBPackConfig(appID string) *DBPackConfig {
	return config.AppConfig[appID]
}

func (conf *DBPackConfig) GetEtcdConfig() *clientv3.Config {
	if conf.DistributedTransaction != nil && conf.DistributedTransaction.EtcdConfig != nil {
		return conf.DistributedTransaction.EtcdConfig
	}
	return nil
}

func (conf *DBPackConfig) Normalize() error {
	if conf.AppID == "" {
		return errors.New("AppID can not be empty")
	}
	if err := conf._validateListeners(); err != nil {
		return err
	}
	if err := conf._validateExecutors(); err != nil {
		return err
	}
	if err := conf._validateDataSources(); err != nil {
		return err
	}
	for _, filter := range conf.Filters {
		filter.AppID = conf.AppID
	}
	if conf.DistributedTransaction != nil {
		conf.DistributedTransaction.AppID = conf.AppID
	}
	return nil
}

func (conf *DBPackConfig) _validateListeners() error {
	for _, listener := range conf.Listeners {
		if listener.Executor != "" {
			var _executor *Executor
			for _, executor := range conf.Executors {
				if executor.Name == listener.Executor {
					_executor = executor
				}
			}
			if _executor == nil {
				return errors.Errorf("Listener %s doesn't have a valid executor", listener.SocketAddress)
			}
		}
		for _, filterName := range listener.Filters {
			var _filter *Filter
			for _, filter := range conf.Filters {
				if filter.Name == filterName {
					_filter = filter
				}
			}
			if _filter == nil {
				return errors.Errorf("Listener %s doesn't have a valid filter %s", listener.SocketAddress, filterName)
			}
		}
		listener.AppID = conf.AppID
	}
	return nil
}

func (conf *DBPackConfig) _validateExecutors() error {
	for _, executor := range conf.Executors {
		for _, filterName := range executor.Filters {
			var _filter *Filter
			for _, filter := range conf.Filters {
				if filter.Name == filterName {
					_filter = filter
				}
			}
			if _filter == nil {
				return errors.Errorf("Executor %s doesn't have a valid filter %s", executor.Name, filterName)
			}
		}
		executor.AppID = conf.AppID
	}
	return nil
}

func (conf *DBPackConfig) _validateDataSources() error {
	for _, dataSource := range conf.DataSources {
		for _, filterName := range dataSource.Filters {
			var _filter *Filter
			for _, filter := range conf.Filters {
				if filter.Name == filterName {
					_filter = filter
				}
			}
			if _filter == nil {
				return errors.Errorf("DataSource %s doesn't have a valid filter %s", dataSource.Name, filterName)
			}
		}
	}
	return nil
}

func (sa SocketAddress) String() string {
	return fmt.Sprintf("%s:%d", sa.Address, sa.Port)
}

var _configuration = new(Configuration)

// Load config file and parse
func Load(path string) (*Configuration, error) {
	configPath, _ := filepath.Abs(path)
	log.Infof("load config from :  %s", configPath)
	content, err := os.ReadFile(configPath)
	if err != nil {
		return nil, errors.Wrap(err, "[config] load config failed")
	}
	configuration, err := _parse(content)
	if err == nil {
		for appID, config := range configuration.AppConfig {
			config.AppID = appID
			if err := config.Normalize(); err != nil {
				return nil, err
			}
		}
		_configuration = configuration
	}
	return configuration, err
}

func GetDBPackConfig(appID string) *DBPackConfig {
	return _configuration.DBPackConfig(appID)
}

func _parse(content []byte) (*Configuration, error) {
	var configuration Configuration
	if err := yaml.Unmarshal(content, &configuration); err != nil {
		return nil, errors.Wrap(err, "[config] yaml unmarshal config failed")
	}
	return &configuration, nil
}
