package metrics_test

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/bolted/database"
	"github.com/draganm/bolted/embedded"
	"github.com/draganm/bolted/metrics"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func openEmptyDatabase(t *testing.T, opts ...embedded.Option) (database.Bolted, func()) {
	td, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	removeTempDir := func() {
		err = os.RemoveAll(td)
		require.NoError(t, err)
	}

	db, err := embedded.Open(filepath.Join(td, "db"), 0660, opts...)

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

func stringOf(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func findMetricWithName(t *testing.T, name string) *dto.Metric {
	metrics, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	for _, m := range metrics {
		if *m.Name == "bolted_number_of_write_transactions_total" {
			for _, met := range m.Metric {
				for _, l := range met.GetLabel() {
					if stringOf(l.Name) == "dbname" && stringOf(l.Value) == t.Name() {
						return met
					}
				}
			}
		}
	}

	require.Failf(t, "metric with name %q not found", name)

	return nil
}

func TestMetrics(t *testing.T) {

	t.Run("number of write transactions", func(t *testing.T) {
		db, cleanupDatabase := openEmptyDatabase(t, embedded.WithWriteTxDecorators(metrics.NewWriteTxDecorator(t.Name())))

		defer cleanupDatabase()

		err := database.SugaredWrite(db, func(tx database.SugaredWriteTx) error {
			return nil
		})

		require.NoError(t, err)

		met := findMetricWithName(t, "bolted_number_of_write_transactions_total")

		require.NotNil(t, met.Counter)

		require.NotNil(t, met.Counter.Value)

		require.Equal(t, 1.0, *met.Counter.Value)
	})

	t.Run("number of successful transactions", func(t *testing.T) {

		db, cleanupDatabase := openEmptyDatabase(t, embedded.WithWriteTxDecorators(metrics.NewWriteTxDecorator(t.Name())))

		defer cleanupDatabase()

		err := database.SugaredWrite(db, func(tx database.SugaredWriteTx) error {
			return nil
		})

		require.NoError(t, err)

		met := findMetricWithName(t, "bolted_number_of_write_transactions_successful")

		require.NotNil(t, met.Counter)

		require.NotNil(t, met.Counter.Value)

		require.Equal(t, 1.0, *met.Counter.Value)
	})

	t.Run("number of failed transactions", func(t *testing.T) {
		db, cleanupDatabase := openEmptyDatabase(t, embedded.WithWriteTxDecorators(metrics.NewWriteTxDecorator(t.Name())))

		defer cleanupDatabase()

		err := database.SugaredWrite(db, func(tx database.SugaredWriteTx) error {
			return errors.New("nope!")
		})

		require.NotNil(t, err)

		met := findMetricWithName(t, "bolted_number_of_write_transactions_failed")

		require.NotNil(t, met.Counter)

		require.NotNil(t, met.Counter.Value)

		require.Equal(t, 1.0, *met.Counter.Value)
	})

}
