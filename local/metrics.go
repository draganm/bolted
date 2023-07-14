package local

import "github.com/prometheus/client_golang/prometheus"

var numberOfWriteTransactionsVec = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "bolted_number_of_write_transactions_total",
	Help: "Total number of write transactions performed on the database",
}, []string{
	"path",
})

var numberOfSuccessfulWriteTransactionsVec = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "bolted_number_of_write_transactions_successful",
	Help: "Number of successful write transactions performed on the database",
}, []string{
	"path",
})

var numberOfFailedTransactionsVec = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "bolted_number_of_write_transactions_failed",
	Help: "Number of failed write transactions performed on the database",
}, []string{
	"path",
})

var dbFileSizeVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "bolted_db_file_size",
	Help: "Size of the database file",
}, []string{
	"path",
})

func init() {
	prometheus.MustRegister(
		numberOfWriteTransactionsVec,
		numberOfSuccessfulWriteTransactionsVec,
		numberOfFailedTransactionsVec,
		dbFileSizeVec,
	)
}

func initializeMetricsForDB(path string, size float64) {
	{
		m, err := numberOfWriteTransactionsVec.GetMetricWithLabelValues(path)
		if err == nil {
			m.Add(0)
		}
	}

	{
		m, err := numberOfSuccessfulWriteTransactionsVec.GetMetricWithLabelValues(path)
		if err == nil {
			m.Add(0)
		}
	}

	{
		m, err := numberOfFailedTransactionsVec.GetMetricWithLabelValues(path)
		if err == nil {
			m.Add(0)
		}
	}

	{
		m, err := dbFileSizeVec.GetMetricWithLabelValues(path)
		if err == nil {
			m.Set(size)
		}
	}

}

func removeMetricsForDB(path string) {
	numberOfWriteTransactionsVec.DeleteLabelValues(path)
	numberOfSuccessfulWriteTransactionsVec.DeleteLabelValues(path)
	numberOfFailedTransactionsVec.DeleteLabelValues(path)
	dbFileSizeVec.DeleteLabelValues(path)

}
