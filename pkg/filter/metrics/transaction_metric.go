package metrics

import "github.com/prometheus/client_golang/prometheus"

const (
	TransactionStatusActive    = "active"
	TransactionStatusCommitted = "committed"
	TransactionStatusRollback  = "rollback"
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
