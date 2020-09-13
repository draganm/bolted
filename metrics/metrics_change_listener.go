package metrics

import (
	"github.com/draganm/bolted"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

type metricsChangeListener string

func NewChangeListener(dbName string) bolted.ChangeListener {
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

func (l metricsChangeListener) Opened(b *bolted.Bolted) error {
	return nil
}

func (l metricsChangeListener) Start(w bolted.WriteTx) error {
	dbname := string(l)

	cnt, err := numberOfWriteTransactionsVec.GetMetricWithLabelValues(dbname)
	if err != nil {
		return errors.Wrap(err, "while getting metric counter")
	}

	cnt.Add(1)
	return nil
}

func (l metricsChangeListener) Delete(w bolted.WriteTx, path string) error {
	return nil
}

func (l metricsChangeListener) CreateMap(w bolted.WriteTx, path string) error {
	return nil
}

func (l metricsChangeListener) Put(w bolted.WriteTx, path string, newValue []byte) error {
	return nil
}

func (l metricsChangeListener) BeforeCommit(w bolted.WriteTx) error {
	return nil
}

func (l metricsChangeListener) AfterTransaction(err error) error {
	dbname := string(l)
	if err != nil {

		cnt, err := numberOfFailedTransactionsVec.GetMetricWithLabelValues(dbname)
		if err != nil {
			return errors.Wrap(err, "while getting metric counter")
		}
		cnt.Inc()
	} else {
		cnt, err := numberOfSuccessfulWriteTransactionsVec.GetMetricWithLabelValues(dbname)
		if err != nil {
			return errors.Wrap(err, "while getting metric counter")
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
