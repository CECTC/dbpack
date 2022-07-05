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
	"io/ioutil"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"gopkg.in/yaml.v3"

	"github.com/cectc/dbpack/pkg/log"
)

type Configuration struct {
	Listeners []*Listener `yaml:"listeners" json:"listeners"`

	Executors []*Executor `yaml:"executors" json:"executors"`

	Filters []*Filter `yaml:"filters" json:"filters"`

	DataSources []*DataSource `yaml:"data_source_cluster" json:"data_source_cluster"`

	DistributedTransaction *DistributedTransaction `yaml:"distributed_transaction" json:"distributed_transaction"`

	TerminationDrainDuration time.Duration `yaml:"termination_drain_duration" json:"termination_drain_duration"`

	HTTPListenPort *int `yaml:"http_listen_port"`

	Trace *Trace `yaml:"trace"`
}

type (
	// ProtocolType protocol type enum
	ProtocolType int32

	// SocketAddress specify either a logical or physical address and port, which are
	// used to tell server where to bind/listen, connect to upstream and find
	// management servers
	SocketAddress struct {
		Address string `default:"0.0.0.0" yaml:"address" json:"address"`
		Port    int    `default:"8881" yaml:"port" json:"port"`
	}

	// Parameters defines a key-value parameters mapping
	Parameters map[string]interface{}

	Filter struct {
		Name   string     `yaml:"name" json:"name"`
		Kind   string     `yaml:"kind" json:"kind"`
		Config Parameters `yaml:"conf,omitempty" json:"conf,omitempty"`
	}

	Executor struct {
		Name    string      `yaml:"name" json:"name"`
		Mode    ExecuteMode `yaml:"mode" json:"mode"`
		Config  Parameters  `yaml:"config" json:"config"`
		Filters []string    `yaml:"filters" json:"filters"`
	}

	Listener struct {
		ProtocolType  ProtocolType  `yaml:"protocol_type" json:"protocol_type"`
		SocketAddress SocketAddress `yaml:"socket_address" json:"socket_address"`
		Filters       []string      `yaml:"filters" json:"filters"`
		Config        Parameters    `yaml:"config" json:"config"`
		Executor      string        `yaml:"executor" json:"executor"`
	}

	// Storage defines the configuration for registry object storage
	Storage map[string]Parameters

	DistributedTransaction struct {
		ApplicationID                    string `yaml:"appid" json:"appid"`
		RetryDeadThreshold               int64  `yaml:"retry_dead_threshold" json:"retry_dead_threshold"`
		RollbackRetryTimeoutUnlockEnable bool   `yaml:"rollback_retry_timeout_unlock_enable" json:"rollback_retry_timeout_unlock_enable"`

		EtcdConfig clientv3.Config `yaml:"etcd_config" json:"etcd_config"`
	}

	EnforcementPolicy struct {
		MinTime             time.Duration `yaml:"min_time" json:"min_time"`
		PermitWithoutStream bool          `yaml:"permit_without_stream" json:"permit_without_stream"`
	}

	ServerParameters struct {
		MaxConnectionIdle     time.Duration `yaml:"max_connection_idle" json:"max_connection_idle"`
		MaxConnectionAge      time.Duration `yaml:"max_connection_age" json:"max_connection_age"`
		MaxConnectionAgeGrace time.Duration `yaml:"max_connection_age_grace" json:"max_connection_age_grace"`
		Time                  time.Duration `yaml:"time" json:"time"`
		Timeout               time.Duration `yaml:"timeout" json:"Timeout"`
	}

	ClientParameters struct {
		Time                time.Duration `yaml:"time" json:"-"`
		Timeout             time.Duration `yaml:"timeout" json:"-"`
		PermitWithoutStream bool          `yaml:"permit_without_stream"`
	}

	Trace struct {
		JaegerEndpoint string `yaml:"jaeger_endpoint"`
	}
)

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

// Type returns the storage driver type, such as filesystem or s3
func (storage Storage) Type() string {
	var storageType []string

	// Return only key in this map
	for k := range storage {
		storageType = append(storageType, k)
	}
	if len(storageType) > 1 {
		log.Panic("multiple storage drivers specified in distributed transaction config: " + strings.Join(storageType, ", "))
	}
	if len(storageType) == 1 {
		return storageType[0]
	}
	return ""
}

// Parameters returns the Parameters map for a Storage configuration
func (storage Storage) Parameters() Parameters {
	return storage[storage.Type()]
}

// setParameter changes the parameter at the provided key to the new value
func (storage Storage) setParameter(key string, value interface{}) {
	storage[storage.Type()][key] = value
}

func parse(content []byte) *Configuration {
	cfg := &Configuration{
		TerminationDrainDuration: time.Second * 3,
	}
	if err := yaml.Unmarshal(content, cfg); err != nil {
		log.Fatalf("[config] [default load] yaml unmarshal config failed, error: %v", err)
	}

	return cfg
}

// Load config file and parse
func Load(path string) *Configuration {
	configPath, _ := filepath.Abs(path)
	log.Infof("load config from :  %s", configPath)
	content, err := ioutil.ReadFile(configPath)
	if err != nil {
		log.Fatalf("[config] [default load] load config failed, error: %v", err)
	}
	return parse(content)
}
