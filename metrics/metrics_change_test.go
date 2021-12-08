package metrics_test

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/bolted"
	"github.com/draganm/bolted/metrics"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func openEmptyDatabase(t *testing.T, opts ...bolted.Option) (*bolted.Bolted, func()) {
	td, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	removeTempDir := func() {
		err = os.RemoveAll(td)
		require.NoError(t, err)
	}

	db, err := bolted.Open(filepath.Join(td, "db"), 0660, opts...)

	require.NoError(t, err)

	closeDatabase := func() {
		err = db.Close()
		require.NoError(t, err)
	}

	return db, func() {
		closeDatabase()
		removeTempDir()
	}

}

func findMetricsWithName(t *testing.T, name string) dto.MetricFamily {
	metrics, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	for _, m := range metrics {
		if *m.Name == "bolted_number_of_write_transactions_total" {
			return *m
		}
	}

	require.Failf(t, "metric with name %q not found", name)

	return dto.MetricFamily{}
}

func TestMetrics(t *testing.T) {

	t.Run("number of write transactions", func(t *testing.T) {
		db, cleanupDatabase := openEmptyDatabase(t, bolted.WithChangeListeners(metrics.NewChangeListener("main")))

		defer cleanupDatabase()

		err := db.Write(func(tx bolted.Write) error {
			return nil
		})

		require.NoError(t, err)

		metFamily := findMetricsWithName(t, "bolted_number_of_write_transactions_total")

		mets := metFamily.Metric
		require.Equal(t, 1, len(mets))

		met := mets[0]

		require.NotNil(t, met.Counter)

		require.NotNil(t, met.Counter.Value)

		require.Equal(t, 1.0, *met.Counter.Value)
	})

	t.Run("number of successful transactions", func(t *testing.T) {
		db, cleanupDatabase := openEmptyDatabase(t, bolted.WithChangeListeners(metrics.NewChangeListener("main")))

		defer cleanupDatabase()

		err := db.Write(func(tx bolted.Write) error {
			return nil
		})

		require.NoError(t, err)

		metFamily := findMetricsWithName(t, "bolted_number_of_write_transactions_successful")

		mets := metFamily.Metric
		require.Equal(t, 1, len(mets))

		met := mets[0]

		require.NotNil(t, met.Counter)

		require.NotNil(t, met.Counter.Value)

		require.Equal(t, 1.0, *met.Counter.Value)
	})

	t.Run("number of failed transactions", func(t *testing.T) {
		db, cleanupDatabase := openEmptyDatabase(t, bolted.WithChangeListeners(metrics.NewChangeListener("main")))

		defer cleanupDatabase()

		err := db.Write(func(tx bolted.Write) error {
			return errors.New("nope!")
		})

		require.NotNil(t, err)

		metFamily := findMetricsWithName(t, "bolted_number_of_write_transactions_failed")

		mets := metFamily.Metric
		require.Equal(t, 1, len(mets))

		met := mets[0]

		require.NotNil(t, met.Counter)

		require.NotNil(t, met.Counter.Value)

		require.Equal(t, 1.0, *met.Counter.Value)
	})

}
