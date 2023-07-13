package metrics

import (
	"github.com/draganm/bolted"
	"github.com/draganm/bolted/local"
	"github.com/prometheus/client_golang/prometheus"
)

type metricsTXWriter struct {
	bolted.WriteTx
	dbName string
	failed bool
}

func NewWriteTxDecorator(dbName string) local.WriteTxDecorator {
	return local.WriteTxDecorator(func(tx bolted.WriteTx) bolted.WriteTx {
		cnt, err := numberOfWriteTransactionsVec.GetMetricWithLabelValues(dbName)
		if err != nil {
			// TODO: log?
		} else {
			cnt.Add(1)
		}

		return &metricsTXWriter{
			WriteTx: tx,
			dbName:  dbName,
		}
	})
}

var numberOfWriteTransactionsVec = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "bolted_number_of_write_transactions_total",
	Help: "Total number of write transactions performed on the database",
}, []string{
	"dbname",
})

var numberOfSuccessfulWriteTransactionsVec = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "bolted_number_of_write_transactions_successful",
	Help: "Number of successful write transactions performed on the database",
}, []string{
	"dbname",
})

var numberOfFailedTransactionsVec = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "bolted_number_of_write_transactions_failed",
	Help: "Number of failed write transactions performed on the database",
}, []string{
	"dbname",
})

func init() {
	prometheus.MustRegister(
		numberOfWriteTransactionsVec,
		numberOfSuccessfulWriteTransactionsVec,
		numberOfFailedTransactionsVec,
	)
}

func (l *metricsTXWriter) OnCommit() {

	cnt, e := numberOfSuccessfulWriteTransactionsVec.GetMetricWithLabelValues(l.dbName)
	if e == nil {
		cnt.Inc()
	}

}

func (l *metricsTXWriter) OnRollback(err error) {
	cnt, e := numberOfFailedTransactionsVec.GetMetricWithLabelValues(l.dbName)
	if e == nil {
		cnt.Inc()
	}
}
