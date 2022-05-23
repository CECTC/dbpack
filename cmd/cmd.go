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
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
	Version = "0.1.0"

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
				factory := filter.GetFilterFactory(filterConf.Name)
				if factory == nil {
					panic(errors.Errorf("there is no filter factory for filter: %s", filterConf.Name))
				}
				f, err := factory.NewFilter(filterConf.Config)
				if err != nil {
					panic(errors.WithMessagef(err, "failed to create filter: %s", filterConf.Name))
				}
				filter.RegisterFilter(f.GetName(), f)
			}

			resource.InitDBManager(conf.DataSources, func(dbName, dsn string) pools.Factory {
				collector, err := driver.NewConnector(dbName, dsn)
				if err != nil {
					panic(err)
				}
				return collector.NewBackendConnection
			})

			executors := make(map[string]proto.Executor)
			for _, executorConf := range conf.Executors {
				if executorConf.Mode == config.SDB {
					executor, err := executor.NewSingleDBExecutor(executorConf)
					if err != nil {
						panic(err)
					}
					executors[executorConf.Name] = executor
				}
				if executorConf.Mode == config.RWS {
					executor, err := executor.NewReadWriteSplittingExecutor(executorConf)
					if err != nil {
						panic(err)
					}
					executors[executorConf.Name] = executor
				}
				if executorConf.Mode == config.SHD {
					executor, err := executor.NewShardingExecutor(executorConf)
					if err != nil {
						panic(err)
					}
					executors[executorConf.Name] = executor
				}
			}

			if conf.DistributedTransaction != nil {
				driver := etcd.NewEtcdStore(conf.DistributedTransaction.EtcdConfig)
				dt.InitDistributedTransactionManager(conf.DistributedTransaction, driver)
			}

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

			dbpack.Start(ctx)

			// init metrics for prometheus server scrape.
			defaultExporterPort := 18888
			var lis net.Listener
			var lisErr error
			if conf.ExporterPort != nil {
				lis, lisErr = net.Listen("tcp4", fmt.Sprintf(":%d", *conf.ExporterPort))
			} else {
				lis, lisErr = net.Listen("tcp4", fmt.Sprintf(":%d", defaultExporterPort))
			}

			if lisErr != nil {
				panic(lisErr)
			}
			go initMetrics(ctx, lis)
		},
	}
)

// init Init startCmd
func init() {
	startCommand.PersistentFlags().StringVarP(&configPath, constant.ConfigPathKey, "c", os.Getenv(constant.EnvDBPackConfig), "Load configuration from `FILE`")
	rootCommand.AddCommand(startCommand)
}

func initMetrics(ctx context.Context, lis net.Listener) {
	go func() {
		<-ctx.Done()
		lis.Close()
	}()
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	httpS := &http.Server{
		Handler: mux,
	}
	err := httpS.Serve(lis)
	if err != nil {
		log.Fatalf("unable create status server: %+v", err)
		return
	}
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
