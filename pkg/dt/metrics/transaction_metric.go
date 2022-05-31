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

package metrics

import "github.com/prometheus/client_golang/prometheus"

const (
	TransactionStatusActive     = "active"
	TransactionStatusCommitted  = "committed"
	TransactionStatusRollbacked = "rollbacked"
	TransactionStatusTimeout    = "timeout"
)

var (
	GlobalTransactionCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "dbpack",
		Subsystem: "global_transaction",
		Name:      "count",
		Help:      "global transaction count",
	}, []string{"appid", "transactionname", "status"})

	BranchTransactionCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "dbpack",
		Subsystem: "branch_transaction",
		Name:      "count",
		Help:      "branch transaction count",
	}, []string{"appid", "resourceid", "status"})

	BranchTransactionTimer = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "dbpack",
		Subsystem: "branch_transaction",
		Name:      "timer",
		Help:      "global transaction timer",
	}, []string{"appid", "resourceid", "status"})
)

func init() {
	prometheus.MustRegister(GlobalTransactionCounter)
	prometheus.MustRegister(BranchTransactionCounter)
	prometheus.MustRegister(BranchTransactionTimer)
}
