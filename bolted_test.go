package bolted_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/bolted"
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

func TestOpen(t *testing.T) {
	_, cleanup := openEmptyDatabase(t)
	defer cleanup()
}

func TestCreateMap(t *testing.T) {

	t.Run("create map", func(t *testing.T) {
		db, cleanup := openEmptyDatabase(t)
		defer cleanup()
		err := db.Write(func(tx bolted.WriteTx) error {
			return tx.CreateMap("test")
		})
		require.NoError(t, err)
	})

	t.Run("create map twice", func(t *testing.T) {
		db, cleanup := openEmptyDatabase(t)
		defer cleanup()

		err := db.Write(func(tx bolted.WriteTx) error {
			return tx.CreateMap("test")
		})
		require.NoError(t, err)

		err = db.Write(func(tx bolted.WriteTx) error {
			return tx.CreateMap("test")
		})

		require.EqualError(t, err, "bucket already exists")
	})

	t.Run("create map nested", func(t *testing.T) {
		db, cleanup := openEmptyDatabase(t)
		defer cleanup()

		err := db.Write(func(tx bolted.WriteTx) error {
			return tx.CreateMap("test")
		})
		require.NoError(t, err)

		err = db.Write(func(tx bolted.WriteTx) error {
			return tx.CreateMap("test/foo")
		})
		require.NoError(t, err)

		err = db.Read(func(tx bolted.ReadTx) error {
			ex, err := tx.Exists("test")
			require.NoError(t, err)
			require.True(t, ex)

			ex, err = tx.Exists("test/foo")
			require.NoError(t, err)
			require.True(t, ex)

			return err
		})

		require.NoError(t, err)
	})

}

func TestDelete(t *testing.T) {

	t.Run("delete not existing map", func(t *testing.T) {
		db, cleanup := openEmptyDatabase(t)
		defer cleanup()

		err := db.Write(func(tx bolted.WriteTx) error {
			return tx.Delete("test")
		})
		require.Equal(t, bolted.ErrNotFound, err)
	})

	t.Run("delete existing map", func(t *testing.T) {
		db, cleanup := openEmptyDatabase(t)
		defer cleanup()

		err := db.Write(func(tx bolted.WriteTx) error {
			return tx.CreateMap("test")
		})

		require.NoError(t, err)
		err = db.Write(func(tx bolted.WriteTx) error {
			return tx.Delete("test")
		})
		require.NoError(t, err)
	})

	t.Run("delete parent map", func(t *testing.T) {
		db, cleanup := openEmptyDatabase(t)
		defer cleanup()

		err := db.Write(func(tx bolted.WriteTx) error {
			err := tx.CreateMap("test")
			if err != nil {
				return err
			}
			return tx.CreateMap("test/foo")
		})

		require.NoError(t, err)
		err = db.Write(func(tx bolted.WriteTx) error {
			return tx.Delete("test")
		})
		require.NoError(t, err)
	})

	t.Run("delete child map", func(t *testing.T) {
		db, cleanup := openEmptyDatabase(t)
		defer cleanup()

		err := db.Write(func(tx bolted.WriteTx) error {
			err := tx.CreateMap("test")
			if err != nil {
				return err
			}
			return tx.CreateMap("test/foo")
		})

		require.NoError(t, err)
		err = db.Write(func(tx bolted.WriteTx) error {
			return tx.Delete("test/foo")
		})
		require.NoError(t, err)
	})

	t.Run("delete value", func(t *testing.T) {
		db, cleanup := openEmptyDatabase(t)
		defer cleanup()

		err := db.Write(func(tx bolted.WriteTx) error {
			return tx.Put("test", []byte{1, 2, 3})
		})

		require.NoError(t, err)
		err = db.Write(func(tx bolted.WriteTx) error {
			return tx.Delete("test")
		})
		require.NoError(t, err)
	})

}

func TestPutAndGet(t *testing.T) {

	t.Run("put and get to root", func(t *testing.T) {
		db, cleanup := openEmptyDatabase(t)
		defer cleanup()

		err := db.Write(func(tx bolted.WriteTx) error {
			return tx.Put("test", []byte{1, 2, 3})
		})
		require.NoError(t, err)

		var val []byte

		err = db.Read(func(tx bolted.ReadTx) error {
			val, err = tx.Get("test")
			return err
		})

		require.NoError(t, err)

		require.Equal(t, []byte{1, 2, 3}, val)

	})

	t.Run("put and get to map root", func(t *testing.T) {
		db, cleanup := openEmptyDatabase(t)
		defer cleanup()

		err := db.Write(func(tx bolted.WriteTx) error {
			err := tx.CreateMap("test")
			if err != nil {
				return err
			}

			return tx.Put("test/foo", []byte{1, 2, 3})
		})
		require.NoError(t, err)

		var val []byte

		err = db.Read(func(tx bolted.ReadTx) error {
			val, err = tx.Get("test/foo")
			return err
		})

		require.NoError(t, err)

		require.Equal(t, []byte{1, 2, 3}, val)

		err = db.Read(func(tx bolted.ReadTx) error {
			ex, err := tx.Exists("test")
			require.NoError(t, err)
			require.True(t, ex)

			isMap, err := tx.IsMap("test")
			require.NoError(t, err)
			require.True(t, isMap)

			cnt, err := tx.Size("test")
			require.NoError(t, err)
			require.Equal(t, uint64(1), cnt)

			ex, err = tx.Exists("test/foo")
			require.NoError(t, err)
			require.True(t, ex)

			isMap, err = tx.IsMap("test/foo")
			require.NoError(t, err)
			require.False(t, isMap)

			cnt, err = tx.Size("test/foo")
			require.NoError(t, err)
			require.Equal(t, uint64(3), cnt)

			return err
		})

		require.NoError(t, err)

	})

}

func TestIterator(t *testing.T) {

	t.Run("iterating empty root", func(t *testing.T) {
		db, cleanup := openEmptyDatabase(t)
		defer cleanup()

		err := db.Write(func(tx bolted.WriteTx) error {
			it, err := tx.Iterator("")
			if err != nil {
				return err
			}
			require.True(t, it.Done)
			return nil
		})
		require.NoError(t, err)

	})

	t.Run("iterating root with one value", func(t *testing.T) {
		db, cleanup := openEmptyDatabase(t)
		defer cleanup()

		err := db.Write(func(tx bolted.WriteTx) error {
			return tx.Put("test", []byte{1, 2, 3})
		})
		require.NoError(t, err)

		err = db.Read(func(tx bolted.ReadTx) error {
			it, err := tx.Iterator("")
			if err != nil {
				return err
			}
			require.False(t, it.Done)

			require.Equal(t, "test", it.Key)
			require.Equal(t, []byte{1, 2, 3}, it.Value)

			it.Next()
			require.True(t, it.Done)

			return nil
		})
		require.NoError(t, err)

	})

	t.Run("iterating root with two values", func(t *testing.T) {
		db, cleanup := openEmptyDatabase(t)
		defer cleanup()

		err := db.Write(func(tx bolted.WriteTx) error {
			err := tx.Put("test1", []byte{1, 2, 3})
			if err != nil {
				return err
			}

			return tx.Put("test2", []byte{2, 3, 4})
		})
		require.NoError(t, err)

		err = db.Read(func(tx bolted.ReadTx) error {
			it, err := tx.Iterator("")
			if err != nil {
				return err
			}

			require.False(t, it.Done)
			require.Equal(t, "test1", it.Key)
			require.Equal(t, []byte{1, 2, 3}, it.Value)

			it.Next()

			require.False(t, it.Done)
			require.Equal(t, "test2", it.Key)
			require.Equal(t, []byte{2, 3, 4}, it.Value)

			it.Prev()

			require.False(t, it.Done)
			require.Equal(t, "test1", it.Key)
			require.Equal(t, []byte{1, 2, 3}, it.Value)

			it.Last()

			require.False(t, it.Done)
			require.Equal(t, "test2", it.Key)
			require.Equal(t, []byte{2, 3, 4}, it.Value)

			it.Next()

			require.True(t, it.Done)

			return nil
		})
		require.NoError(t, err)

	})

	t.Run("iterating root with two values and a bucket", func(t *testing.T) {
		db, cleanup := openEmptyDatabase(t)
		defer cleanup()

		err := db.Write(func(tx bolted.WriteTx) error {
			err := tx.Put("test1", []byte{1, 2, 3})
			if err != nil {
				return err
			}

			err = tx.Put("test2", []byte{2, 3, 4})
			if err != nil {
				return err
			}

			return tx.CreateMap("test3")

		})
		require.NoError(t, err)

		err = db.Read(func(tx bolted.ReadTx) error {
			it, err := tx.Iterator("")
			if err != nil {
				return err
			}

			require.False(t, it.Done)
			require.Equal(t, "test1", it.Key)
			require.Equal(t, []byte{1, 2, 3}, it.Value)

			it.Next()

			require.False(t, it.Done)
			require.Equal(t, "test2", it.Key)
			require.Equal(t, []byte{2, 3, 4}, it.Value)

			it.Next()

			require.False(t, it.Done)
			require.Equal(t, "test3", it.Key)
			require.Equal(t, []byte(nil), it.Value)

			it.Next()

			require.True(t, it.Done)

			return nil
		})
		require.NoError(t, err)

	})

}
