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

package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	"github.com/cectc/dbpack/pkg/config"
	"github.com/cectc/dbpack/pkg/constant"
	"github.com/cectc/dbpack/pkg/driver"
	"github.com/cectc/dbpack/pkg/dt"
	"github.com/cectc/dbpack/pkg/dt/storage/etcd"
	"github.com/cectc/dbpack/pkg/executor"
	"github.com/cectc/dbpack/pkg/filter"
	_ "github.com/cectc/dbpack/pkg/filter/dt"
	_ "github.com/cectc/dbpack/pkg/filter/metrics"
	dbpackHttp "github.com/cectc/dbpack/pkg/http"
	"github.com/cectc/dbpack/pkg/listener"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/proto"
	"github.com/cectc/dbpack/pkg/resource"
	"github.com/cectc/dbpack/pkg/server"
	"github.com/cectc/dbpack/third_party/pools"
	_ "github.com/cectc/dbpack/third_party/types/parser_driver"
)

func main() {
	rootCommand.Execute()
}

var (
	Version               = "0.1.0"
	defaultHTTPListenPort = 18888

	configPath string

	rootCommand = &cobra.Command{
		Use:     "dbpack",
		Short:   "dbpack is a db proxy server",
		Version: Version,
	}

	startCommand = &cobra.Command{
		Use:   "start",
		Short: "start dbpack",

		Run: func(cmd *cobra.Command, args []string) {
			//h := initHolmes()
			//h.Start()
			conf := config.Load(configPath)

			for _, filterConf := range conf.Filters {
				factory := filter.GetFilterFactory(filterConf.Kind)
				if factory == nil {
					log.Fatalf("there is no filter factory for filter: %s", filterConf.Kind)
				}
				f, err := factory.NewFilter(filterConf.Config)
				if err != nil {
					log.Fatal(errors.WithMessagef(err, "failed to create filter: %s", filterConf.Name))
				}
				filter.RegisterFilter(filterConf.Name, f)
			}

			resource.InitDBManager(conf.DataSources, func(dbName, dsn string) pools.Factory {
				collector, err := driver.NewConnector(dbName, dsn)
				if err != nil {
					log.Fatal(err)
				}
				return collector.NewBackendConnection
			})

			executors := make(map[string]proto.Executor)
			for _, executorConf := range conf.Executors {
				if executorConf.Mode == config.SDB {
					executor, err := executor.NewSingleDBExecutor(executorConf)
					if err != nil {
						log.Fatal(err)
					}
					executors[executorConf.Name] = executor
				}
				if executorConf.Mode == config.RWS {
					executor, err := executor.NewReadWriteSplittingExecutor(executorConf)
					if err != nil {
						log.Fatal(err)
					}
					executors[executorConf.Name] = executor
				}
				if executorConf.Mode == config.SHD {
					executor, err := executor.NewShardingExecutor(executorConf)
					if err != nil {
						log.Fatal(err)
					}
					executors[executorConf.Name] = executor
				}
			}

			if conf.DistributedTransaction != nil {
				dbpackHttp.DistributedTransactionEnabled = true
				driver := etcd.NewEtcdStore(conf.DistributedTransaction.EtcdConfig)
				dt.InitDistributedTransactionManager(conf.DistributedTransaction, driver)
			}

			dbpackHttp.Listeners = conf.Listeners
			dbpack := server.NewServer()
			for _, listenerConf := range conf.Listeners {
				switch listenerConf.ProtocolType {
				case config.Mysql:
					listener, err := listener.NewMysqlListener(listenerConf)
					if err != nil {
						log.Fatalf("create mysql listener failed %v", err)
					}
					dbListener := listener.(proto.DBListener)
					executor := executors[listenerConf.Executor]
					if executor == nil {
						log.Fatalf("executor: %s is not exists for mysql listener", listenerConf.Executor)
					}
					dbListener.SetExecutor(executor)
					dbpack.AddListener(dbListener)
				case config.Http:
					listener, err := listener.NewHttpListener(listenerConf)
					if err != nil {
						log.Fatalf("create http listener failed %v", err)
					}
					dbpack.AddListener(listener)
				default:
					log.Fatalf("unsupported %v listener protocol type", listenerConf.ProtocolType)
				}
			}

			ctx, cancel := context.WithCancel(context.Background())
			c := make(chan os.Signal, 2)
			signal.Notify(c, os.Interrupt, syscall.SIGTERM)
			go func() {
				<-c
				go func() {
					// cancel server after sleeping `TerminationDrainDuration`
					// cancel asynchronously to avoid blocking the second term signal
					time.Sleep(conf.TerminationDrainDuration)
					cancel()
				}()
				<-c
				os.Exit(1) // second signal. Exit directly.
			}()

			// init metrics for prometheus server scrape.
			// default listen at 18888
			var lis net.Listener
			var lisErr error
			if conf.HTTPListenPort != nil {
				lis, lisErr = net.Listen("tcp4", fmt.Sprintf(":%d", *conf.HTTPListenPort))
			} else {
				lis, lisErr = net.Listen("tcp4", fmt.Sprintf(":%d", defaultHTTPListenPort))
			}

			if lisErr != nil {
				log.Fatalf("unable init metrics server: %+v", lisErr)
			}
			go initServer(ctx, lis)

			dbpack.Start(ctx)
		},
	}
)

// init Init startCmd
func init() {
	startCommand.PersistentFlags().StringVarP(&configPath, constant.ConfigPathKey, "c", os.Getenv(constant.EnvDBPackConfig), "Load configuration from `FILE`")
	rootCommand.AddCommand(startCommand)
}

func initServer(ctx context.Context, lis net.Listener) {
	go func() {
		<-ctx.Done()
		lis.Close()
	}()
	handler, err := dbpackHttp.RegisterRoutes()
	if err != nil {
		log.Fatalf("failed to init handler: %+v", err)
		return
	}
	httpS := &http.Server{
		Handler: handler,
	}
	err = httpS.Serve(lis)
	if err != nil {
		log.Fatalf("unable create status server: %+v", err)
		return
	}
	log.Infof("start api server :  %s", lis.Addr())
}

//func initHolmes() *holmes.Holmes {
//	logUtils.DefaultLogger.SetLogLevel(logUtils.ERROR)
//	h, _ := holmes.New(
//		holmes.WithCollectInterval("5s"),
//		holmes.WithDumpPath("/tmp"),
//		holmes.WithCPUDump(20, 25, 80, time.Minute),
//		holmes.WithCPUMax(90),
//	)
//	h.EnableCPUDump()
//	return h
//}
