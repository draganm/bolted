package bolted_test

import (
	"testing"

	"github.com/draganm/bolted"
	"github.com/stretchr/testify/require"
)

func TestObservePath(t *testing.T) {

	db, cleanupDatabase := openEmptyDatabase(t)

	defer cleanupDatabase()

	updates, close, err := db.ObservePath("foo")
	require.NoError(t, err)

	defer close()

	t.Run("notification of created map", func(t *testing.T) {
		err := db.Write(func(tx bolted.WriteTx) error {
			return tx.CreateMap("foo")
		})
		require.NoError(t, err)

		ev := <-updates

		require.Equal(t, bolted.ObservedEvent{
			"foo": bolted.MapCreated,
		}, ev)

	})

	t.Run("notification of set value", func(t *testing.T) {
		err := db.Write(func(tx bolted.WriteTx) error {
			return tx.Put("foo/bar", []byte{1, 2, 3})
		})
		require.NoError(t, err)

		ev := <-updates

		require.Equal(t, bolted.ObservedEvent{
			"foo/bar": bolted.ValueSet,
		}, ev)
	})

	t.Run("notification of deleted value", func(t *testing.T) {
		err := db.Write(func(tx bolted.WriteTx) error {
			return tx.Delete("foo/bar")
		})
		require.NoError(t, err)

		ev := <-updates

		require.Equal(t, bolted.ObservedEvent{
			"foo/bar": bolted.Deleted,
		}, ev)
	})

	t.Run("notification of storing value and deleting", func(t *testing.T) {
		err := db.Write(func(tx bolted.WriteTx) error {
			err := tx.Put("foo/bar", []byte{1, 2, 3})
			if err != nil {
				return err
			}

			return tx.Delete("foo")
		})
		require.NoError(t, err)

		ev := <-updates

		require.Equal(t, bolted.ObservedEvent{
			"foo": bolted.Deleted,
		}, ev)
	})

	t.Run("notification of storing value, deleting and re-creating", func(t *testing.T) {
		err := db.Write(func(tx bolted.WriteTx) error {
			err := tx.CreateMap("foo")
			if err != nil {
				return err
			}

			err = tx.Put("foo/bar", []byte{1, 2, 3})
			if err != nil {
				return err
			}

			err = tx.Delete("foo")
			if err != nil {
				return err
			}

			err = tx.CreateMap("foo")
			if err != nil {
				return err
			}

			return tx.Put("foo/bar", []byte{1, 2, 3})
		})
		require.NoError(t, err)

		ev := <-updates

		require.Equal(t, bolted.ObservedEvent{
			"foo":     bolted.MapCreated,
			"foo/bar": bolted.ValueSet,
		}, ev)
	})

}
