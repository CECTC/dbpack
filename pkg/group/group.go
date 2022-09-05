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

package group

import (
	"context"
	"math/rand"
	"strings"
	"time"

	"github.com/uber-go/atomic"

	"github.com/cectc/dbpack/pkg/config"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/mysql"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/pkg/resource"
)

type DBGroup struct {
	groupName string
	masters   []proto.DB
	slaves    []proto.DB

	algorithm    config.LoadBalanceAlgorithm
	writeCounter *atomic.Int64
	readCounter  *atomic.Int64
}

func NewDBGroup(appid, name string,
	algorithm config.LoadBalanceAlgorithm,
	dataSources []*config.DataSourceRef) (proto.DBGroupExecutor, error) {
	var (
		masters = make([]proto.DB, 0)
		slaves  = make([]proto.DB, 0)
	)
	for _, dataSource := range dataSources {
		readWeight, writeWeight, err := dataSource.ParseWeight()
		if err != nil {
			return nil, err
		}
		db := resource.GetDBManager(appid).GetDB(dataSource.Name)
		db.SetWriteWeight(writeWeight)
		db.SetReadWeight(readWeight)
		if db.IsMaster() {
			masters = append(masters, db)
		} else {
			slaves = append(slaves, db)
		}
	}
	return &DBGroup{
		groupName:    name,
		masters:      masters,
		slaves:       slaves,
		algorithm:    algorithm,
		writeCounter: atomic.NewInt64(0),
		readCounter:  atomic.NewInt64(0),
	}, nil
}

func (group *DBGroup) GroupName() string {
	return group.groupName
}

func (group *DBGroup) Begin(ctx context.Context) (proto.Tx, proto.Result, error) {
	dbs := group.getAvailableMasters()
	return dbs[0].Begin(ctx)
}

func (group *DBGroup) XAStart(ctx context.Context, sql string) (proto.Tx, proto.Result, error) {
	dbs := group.getAvailableMasters()
	return dbs[0].XAStart(ctx, sql)
}

func (group *DBGroup) Query(ctx context.Context, query string) (proto.Result, uint16, error) {
	db := group.pick(ctx)
	return db.Query(ctx, query)
}

func (group *DBGroup) QueryAll(ctx context.Context, query string) (proto.Result, uint16, error) {
	queryFunc := func(db proto.DB) {
		if _, _, err := db.Query(ctx, query); err != nil {
			log.Error(err)
		}
	}
	for _, master := range group.masters {
		go queryFunc(master)
	}
	for _, slave := range group.slaves {
		go queryFunc(slave)
	}
	return &mysql.Result{
		AffectedRows: 0,
		InsertId:     0,
	}, 0, nil
}

func (group *DBGroup) Execute(ctx context.Context, query string) (proto.Result, uint16, error) {
	db := group.pick(ctx)
	return db.Query(ctx, query)
}

func (group *DBGroup) PrepareQuery(ctx context.Context, query string, args ...interface{}) (proto.Result, uint16, error) {
	db := group.pick(ctx)
	return db.ExecuteSql(ctx, query, args...)
}

func (group *DBGroup) PrepareExecute(ctx context.Context, query string, args ...interface{}) (proto.Result, uint16, error) {
	db := group.pick(ctx)
	return db.ExecuteSql(ctx, query, args...)
}

func (group *DBGroup) PrepareExecuteStmt(ctx context.Context, stmt *proto.Stmt) (proto.Result, uint16, error) {
	db := group.pick(ctx)
	return db.ExecuteStmt(ctx, stmt)
}

func (group *DBGroup) AddDB(db proto.DB) {
	if db.IsMaster() {
		group.masters = append(group.masters, db)
	} else {
		for _, master := range group.masters {
			if strings.EqualFold(master.Name(), db.MasterName()) {
				group.slaves = append(group.slaves, db)
			}
		}
	}
}

func (group *DBGroup) RemoveDB(name string) {
	masters := make([]proto.DB, 0)
	for _, master := range group.masters {
		if !strings.EqualFold(master.Name(), name) {
			masters = append(masters, master)
		}
	}
	group.masters = masters

	slaves := make([]proto.DB, 0)
	for _, slave := range group.slaves {
		if !strings.EqualFold(slave.Name(), name) {
			slaves = append(slaves, slave)
		}
	}
	group.slaves = slaves
}

func (group *DBGroup) pick(ctx context.Context) proto.DB {
	switch group.algorithm {
	case config.Random:
		return group.random(ctx)
	case config.RoundRobin:
		return group.roundRobin(ctx)
	case config.RandomWeight:
		return group.randomWeight(ctx)
	default:
		return nil
	}
}

func (group *DBGroup) random(ctx context.Context) proto.DB {
	if proto.IsSlave(ctx) {
		slaves := group.getAvailableSlaves()
		if len(slaves) == 0 {
			return group._randomMaster()
		} else if len(slaves) == 1 {
			return slaves[0]
		} else {
			index := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(len(slaves))
			return slaves[index]
		}
	}
	return group._randomMaster()
}

func (group *DBGroup) roundRobin(ctx context.Context) proto.DB {
	if proto.IsSlave(ctx) {
		slaves := group.getAvailableSlaves()
		if len(slaves) == 0 {
			return group._roundRobinMaster()
		} else if len(slaves) == 1 {
			return slaves[0]
		} else {
			index := group.readCounter.Load() % int64(len(group.masters))
			group.readCounter.Inc()
			return slaves[index]
		}
	}
	return group._roundRobinMaster()
}

func (group *DBGroup) randomWeight(ctx context.Context) proto.DB {
	if proto.IsSlave(ctx) {
		dbs := make([]proto.DB, 0)
		weights := make([]int, 0)
		totalWeight := 0
		masters := group.getAvailableMasters()
		for _, db := range masters {
			dbs = append(dbs, db)
			weights = append(weights, db.ReadWeight())
			totalWeight = totalWeight + db.ReadWeight()
		}
		slaves := group.getAvailableSlaves()
		for _, db := range slaves {
			dbs = append(dbs, db)
			weights = append(weights, db.ReadWeight())
			totalWeight = totalWeight + db.ReadWeight()
		}
		if len(dbs) == 1 {
			return slaves[0]
		} else {
			weightSum := 0
			index := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(totalWeight)
			for i := 0; i < len(weights); i++ {
				if index >= weightSum && index < weightSum+weights[i] {
					return dbs[i]
				}
				weightSum += weights[i]
			}
		}
	}
	return group._randomWeightMaster()
}

func (group *DBGroup) _randomMaster() proto.DB {
	dbs := group.getAvailableMasters()
	if len(dbs) == 1 {
		return dbs[0]
	} else {
		index := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(len(dbs))
		return dbs[index]
	}
}

func (group *DBGroup) _roundRobinMaster() proto.DB {
	dbs := group.getAvailableMasters()
	if len(dbs) == 1 {
		return dbs[0]
	} else {
		index := group.writeCounter.Load() % int64(len(dbs))
		group.writeCounter.Inc()
		return dbs[index]
	}
}

func (group *DBGroup) _randomWeightMaster() proto.DB {
	dbs := make([]proto.DB, 0)
	weights := make([]int, 0)
	totalWeight := 0
	for _, db := range group.masters {
		if db.Status() == proto.Running {
			dbs = append(dbs, db)
			weights = append(weights, db.WriteWeight())
			totalWeight = totalWeight + db.WriteWeight()
		}
	}
	if len(dbs) == 1 {
		return dbs[0]
	} else {
		weightSum := 0
		index := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(totalWeight)
		for i := 0; i < len(weights); i++ {
			if index >= weightSum && index < weightSum+weights[i] {
				return dbs[i]
			}
			weightSum += weights[i]
		}
	}
	return nil
}

func (group *DBGroup) getAvailableMasters() []proto.DB {
	dbs := make([]proto.DB, 0)
	for _, db := range group.masters {
		if db.Status() == proto.Running {
			dbs = append(dbs, db)
		}
	}
	return dbs
}

func (group *DBGroup) getAvailableSlaves() []proto.DB {
	slaves := make([]proto.DB, 0)
	for _, slave := range group.slaves {
		if slave.Status() == proto.Running {
			slaves = append(slaves, slave)
		}
	}
	return slaves
}
