package metrics

import (
	"github.com/draganm/bolted/database"
	"github.com/draganm/bolted/embedded"
	"github.com/prometheus/client_golang/prometheus"
)

type metricsTXWriter struct {
	database.WriteTx
	dbName string
	failed bool
}

func NewWriteTxDecorator(dbName string) embedded.WriteTxDecorator {
	return embedded.WriteTxDecorator(func(tx database.WriteTx) database.WriteTx {
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

func (l *metricsTXWriter) Finish() error {
	err := l.WriteTx.Finish()
	if err != nil {
		cnt, e := numberOfFailedTransactionsVec.GetMetricWithLabelValues(l.dbName)
		if e == nil {
			cnt.Inc()
		}
		return err
	}

	if l.failed {
		cnt, e := numberOfFailedTransactionsVec.GetMetricWithLabelValues(l.dbName)
		if e == nil {
			cnt.Inc()
		}
	} else {
		cnt, e := numberOfSuccessfulWriteTransactionsVec.GetMetricWithLabelValues(l.dbName)
		if e == nil {
			cnt.Inc()
		}
	}

	return nil

}

func (l *metricsTXWriter) Rollback() error {
	err := l.WriteTx.Rollback()

	l.failed = true

	return err

}
