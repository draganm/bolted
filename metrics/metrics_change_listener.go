package metrics

import (
	"fmt"

	"github.com/draganm/bolted/database"
	"github.com/draganm/bolted/dbpath"
	"github.com/draganm/bolted/embedded"
	"github.com/prometheus/client_golang/prometheus"
)

type metricsChangeListener string

func NewChangeListener(dbName string) embedded.ChangeListener {
	return metricsChangeListener(dbName)
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

func (l metricsChangeListener) Opened(b *embedded.Bolted) error {
	return nil
}

func (l metricsChangeListener) Start(w database.WriteTx) error {
	dbname := string(l)

	cnt, err := numberOfWriteTransactionsVec.GetMetricWithLabelValues(dbname)
	if err != nil {
		return fmt.Errorf("while getting metric counter: %w", err)
	}

	cnt.Add(1)
	return nil
}

func (l metricsChangeListener) Delete(w database.WriteTx, path dbpath.Path) error {
	return nil
}

func (l metricsChangeListener) CreateMap(w database.WriteTx, path dbpath.Path) error {
	return nil
}

func (l metricsChangeListener) Put(w database.WriteTx, path dbpath.Path, newValue []byte) error {
	return nil
}

func (l metricsChangeListener) BeforeCommit(w database.WriteTx) error {
	return nil
}

func (l metricsChangeListener) AfterTransaction(err error) error {
	dbname := string(l)
	if err != nil {

		cnt, err := numberOfFailedTransactionsVec.GetMetricWithLabelValues(dbname)
		if err != nil {
			return fmt.Errorf("while getting metric counter: %w", err)
		}
		cnt.Inc()
	} else {
		cnt, err := numberOfSuccessfulWriteTransactionsVec.GetMetricWithLabelValues(dbname)
		if err != nil {
			return fmt.Errorf("while getting metric counter: %w", err)
		}
		cnt.Inc()
	}
	return nil
}

func (l metricsChangeListener) Closed() error {
	dbname := string(l)
	numberOfWriteTransactionsVec.DeleteLabelValues(dbname)
	numberOfSuccessfulWriteTransactionsVec.DeleteLabelValues(dbname)
	numberOfFailedTransactionsVec.DeleteLabelValues(dbname)
	return nil
}
