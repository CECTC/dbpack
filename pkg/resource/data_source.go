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

package resource

import (
	"fmt"

	"github.com/cectc/dbpack/pkg/config"
	"github.com/cectc/dbpack/pkg/filter"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/pkg/sql"
	"github.com/cectc/dbpack/third_party/pools"
)

var managers = make(map[string]proto.DBManager)

type DBManager struct {
	dataSources   []*config.DataSource
	resourcePools map[string]proto.DB
}

func (manager *DBManager) GetDB(name string) proto.DB {
	return manager.resourcePools[name]
}

func RegisterDBManager(appid string, dataSources []*config.DataSource, factory func(dbName, dsn string) pools.Factory) {
	resourcePools := make(map[string]proto.DB, 0)

	initResourcePool := func(dataSourceConfig *config.DataSource) *pools.ResourcePool {
		resourcePool := pools.NewResourcePool(factory(dataSourceConfig.Name, dataSourceConfig.DSN), dataSourceConfig.Capacity,
			dataSourceConfig.MaxCapacity, dataSourceConfig.IdleTimeout, 0, nil)
		return resourcePool
	}

	for i := 0; i < len(dataSources); i++ {
		var (
			connectionPreFilters  []proto.DBConnectionPreFilter
			connectionPostFilters []proto.DBConnectionPostFilter
		)
		dataSource := dataSources[i]
		resourcePool := initResourcePool(dataSource)
		db := sql.NewDB(dataSource.Name, dataSource.PingInterval, dataSource.PingTimesForChangeStatus, resourcePool)
		for j := 0; j < len(dataSource.Filters); j++ {
			filterName := dataSource.Filters[j]
			f := filter.GetFilter(filterName)
			if f != nil {
				preFilter, ok := f.(proto.DBConnectionPreFilter)
				if ok {
					connectionPreFilters = append(connectionPreFilters, preFilter)
				}
				postFilter, ok := f.(proto.DBConnectionPostFilter)
				if ok {
					connectionPostFilters = append(connectionPostFilters, postFilter)
				}
			}
		}

		db.SetConnectionPreFilters(connectionPreFilters)
		db.SetConnectionPostFilters(connectionPostFilters)
		resourcePools[dataSource.Name] = db
	}
	managers[appid] = &DBManager{
		dataSources:   dataSources,
		resourcePools: resourcePools,
	}
}

func GetDBManager(appid string) proto.DBManager {
	return managers[appid]
}

func SetDBManager(appid string, manager proto.DBManager) {
	managers[appid] = manager
}

func DetectDBs() error {
	for _, manager := range managers {
		dbManager := manager.(*DBManager)
		for _, db := range dbManager.resourcePools {
			if err := db.Ping(); err != nil {
				return fmt.Errorf("datasource %s is not ready, err: %+v", db.Name(), err)
			}
		}
	}
	return nil
}
