package bolted_test

import (
	"errors"
	"testing"

	"github.com/draganm/bolted"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func findMetricWithName(t *testing.T, name string) *dto.Metric {
	metrics, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	for _, m := range metrics {
		if *m.Name == name {
			for _, met := range m.Metric {
				return met
			}
		}
	}

	require.Failf(t, "metric with name %q not found", name)

	return nil
}

func TestMetrics(t *testing.T) {

	t.Run("number of write transactions", func(t *testing.T) {
		db, cleanupDatabase := openEmptyDatabase(t, bolted.Options{})

		defer cleanupDatabase()

		err := db.Write(ctx, func(tx bolted.WriteTx) error {
			return nil
		})

		require.NoError(t, err)

		met := findMetricWithName(t, "bolted_number_of_write_transactions_total")

		require.NotNil(t, met.Counter)

		require.NotNil(t, met.Counter.Value)

		require.Equal(t, 1.0, *met.Counter.Value)
	})

	t.Run("number of successful transactions", func(t *testing.T) {

		db, cleanupDatabase := openEmptyDatabase(t, bolted.Options{})

		defer cleanupDatabase()

		err := db.Write(ctx, func(tx bolted.WriteTx) error {
			return nil
		})

		require.NoError(t, err)

		met := findMetricWithName(t, "bolted_number_of_write_transactions_successful")

		require.NotNil(t, met.Counter)

		require.NotNil(t, met.Counter.Value)

		require.Equal(t, 1.0, *met.Counter.Value)
	})

	t.Run("number of failed transactions", func(t *testing.T) {
		db, cleanupDatabase := openEmptyDatabase(t, bolted.Options{})

		defer cleanupDatabase()

		err := db.Write(ctx, func(tx bolted.WriteTx) error {
			return errors.New("nope!")
		})

		require.NotNil(t, err)

		met := findMetricWithName(t, "bolted_number_of_write_transactions_failed")

		require.NotNil(t, met.Counter)

		require.NotNil(t, met.Counter.Value)

		require.Equal(t, 1.0, *met.Counter.Value)
	})

	t.Run("db file size", func(t *testing.T) {
		_, cleanupDatabase := openEmptyDatabase(t, bolted.Options{})

		defer cleanupDatabase()

		met := findMetricWithName(t, "bolted_db_file_size")

		require.NotNil(t, met.Gauge)

		require.NotNil(t, met.Gauge.Value)

		require.Greater(t, *met.Gauge.Value, 1.0)
	})

}
