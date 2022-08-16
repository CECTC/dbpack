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

package testdata

import (
	"context"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/cectc/dbpack/pkg/config"
	"github.com/cectc/dbpack/pkg/driver"
	"github.com/cectc/dbpack/pkg/resource"
	"github.com/cectc/dbpack/third_party/pools"
)

const (
	rootPassword = "123456"
)

type ShardingTestEnvironment struct {
	mysql1 testcontainers.Container
	mysql2 testcontainers.Container
}

func NewShardingTestEnvironment(t *testing.T) *ShardingTestEnvironment {
	container1, err := newMySql(t, "world_0.sql", "world-0")
	if err != nil {
		t.Fatal(err)
	}
	container2, err := newMySql(t, "world_1.sql", "world-1")
	if err != nil {
		t.Fatal(err)
	}
	return &ShardingTestEnvironment{mysql1: container1, mysql2: container2}
}

func newMySql(t *testing.T, initSql, name string) (testcontainers.Container, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	scripts := path.Dir(fmt.Sprintf("%s/../../docker/scripts/", cwd))
	source := path.Join(scripts, initSql)
	req := testcontainers.ContainerRequest{
		Name:  name,
		Image: "mysql:8.0",
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": rootPassword,
		},
		ExposedPorts: []string{"3306/tcp"},
		Mounts: testcontainers.ContainerMounts{
			{
				Source:   testcontainers.GenericBindMountSource{HostPath: source},
				Target:   "/docker-entrypoint-initdb.d/init.sql",
				ReadOnly: false,
			},
		},
		WaitingFor: wait.ForListeningPort("3306/tcp"),
	}
	ctx := context.Background()
	t.Logf("Starting %s", name)
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}
	t.Logf("Started %s", name)
	return container, err
}

func (environment *ShardingTestEnvironment) Shutdown(t *testing.T) {
	minute := time.Minute
	err := environment.mysql1.Stop(context.Background(), &minute)
	if err != nil {
		t.Logf("Shutdown dbpack-mysql1 failed, err: %v", err)
	}
	err = environment.mysql2.Stop(context.Background(), &minute)
	if err != nil {
		t.Logf("Shutdown dbpack-mysql2 failed, err: %v", err)
	}
}

func (environment *ShardingTestEnvironment) RegisterDBResource(t *testing.T) {
	t.Log("Init DataSources")
	port1, err := environment.mysql1.MappedPort(context.Background(), "3306/tcp")
	if err != nil {
		t.Fatal(err)
	}
	port2, err := environment.mysql2.MappedPort(context.Background(), "3306/tcp")
	if err != nil {
		t.Fatal(err)
	}
	resource.RegisterDBManager("test", []*config.DataSource{
		{
			Name:                     "world_0",
			DSN:                      fmt.Sprintf("root:123456@tcp(localhost:%d)/world", port1.Int()),
			Capacity:                 10,
			MaxCapacity:              20,
			IdleTimeout:              time.Minute,
			PingInterval:             20 * time.Second,
			PingTimesForChangeStatus: 3,
			Filters:                  nil,
		},
		{
			Name:                     "world_1",
			DSN:                      fmt.Sprintf("root:123456@tcp(localhost:%d)/world", port2.Int()),
			Capacity:                 10,
			MaxCapacity:              20,
			IdleTimeout:              time.Minute,
			PingInterval:             20 * time.Second,
			PingTimesForChangeStatus: 3,
			Filters:                  nil,
		},
	}, func(dbName, dsn string) pools.Factory {
		collector, err := driver.NewConnector(dbName, dsn)
		if err != nil {
			t.Fatal(err)
		}
		return collector.NewBackendConnection
	})
}
