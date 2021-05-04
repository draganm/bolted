package observer_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/bolted"
	"github.com/draganm/bolted/observer"
	"github.com/stretchr/testify/require"
)

var _ bolted.ChangeListener = &observer.Observer{}

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

func TestObservePath(t *testing.T) {

	o := observer.New()

	db, cleanupDatabase := openEmptyDatabase(t, bolted.WithChangeListeners(o))

	defer cleanupDatabase()

	updates, close := o.ObservePath("foo")

	defer close()

	t.Run("notification of created map", func(t *testing.T) {
		err := db.Write(func(tx bolted.WriteTx) error {
			return tx.CreateMap("foo")
		})
		require.NoError(t, err)

		ev := <-updates

		require.Equal(t, observer.ObservedEvent{
			"foo": observer.MapCreated,
		}, ev)

	})

	t.Run("notification of set value", func(t *testing.T) {
		err := db.Write(func(tx bolted.WriteTx) error {
			return tx.Put("foo/bar", []byte{1, 2, 3})
		})
		require.NoError(t, err)

		ev := <-updates

		require.Equal(t, observer.ObservedEvent{
			"foo/bar": observer.ValueSet,
		}, ev)
	})

	t.Run("notification of deleted value", func(t *testing.T) {
		err := db.Write(func(tx bolted.WriteTx) error {
			return tx.Delete("foo/bar")
		})
		require.NoError(t, err)

		ev := <-updates

		require.Equal(t, observer.ObservedEvent{
			"foo/bar": observer.Deleted,
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

		require.Equal(t, observer.ObservedEvent{
			"foo": observer.Deleted,
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

		require.Equal(t, observer.ObservedEvent{
			"foo":     observer.MapCreated,
			"foo/bar": observer.ValueSet,
		}, ev)
	})

}
