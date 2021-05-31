package bolted_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/bolted"
	"github.com/draganm/bolted/dbpath"
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
			tx.CreateMap(dbpath.ToPath("test"))
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("create map twice", func(t *testing.T) {
		db, cleanup := openEmptyDatabase(t)
		defer cleanup()

		err := db.Write(func(tx bolted.WriteTx) error {
			tx.CreateMap(dbpath.ToPath("test"))
			return nil
		})
		require.NoError(t, err)

		err = db.Write(func(tx bolted.WriteTx) error {
			tx.CreateMap(dbpath.ToPath("test"))
			return nil
		})

		require.EqualError(t, err, "CreateMap(test): bucket already exists")
	})

	t.Run("create map nested", func(t *testing.T) {
		db, cleanup := openEmptyDatabase(t)
		defer cleanup()

		err := db.Write(func(tx bolted.WriteTx) error {
			tx.CreateMap(dbpath.ToPath("test"))
			return nil
		})
		require.NoError(t, err)

		err = db.Write(func(tx bolted.WriteTx) error {
			tx.CreateMap(dbpath.ToPath("test", "foo"))
			return nil
		})
		require.NoError(t, err)

		err = db.Read(func(tx bolted.ReadTx) error {
			ex := tx.Exists(dbpath.ToPath("test"))
			require.True(t, ex)

			ex = tx.Exists(dbpath.ToPath("test", "foo"))
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
			tx.Delete(dbpath.ToPath("test"))
			return nil
		})
		require.True(t, bolted.IsNotFound(err))
	})

	t.Run("delete existing map", func(t *testing.T) {
		db, cleanup := openEmptyDatabase(t)
		defer cleanup()

		err := db.Write(func(tx bolted.WriteTx) error {
			tx.CreateMap(dbpath.ToPath("test"))
			return nil
		})

		require.NoError(t, err)
		err = db.Write(func(tx bolted.WriteTx) error {
			tx.Delete(dbpath.ToPath("test"))
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("delete parent map", func(t *testing.T) {
		db, cleanup := openEmptyDatabase(t)
		defer cleanup()

		err := db.Write(func(tx bolted.WriteTx) error {
			tx.CreateMap(dbpath.ToPath("test"))
			tx.CreateMap(dbpath.ToPath("test", "foo"))
			return nil
		})

		require.NoError(t, err)
		err = db.Write(func(tx bolted.WriteTx) error {
			tx.Delete(dbpath.ToPath("test"))
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("delete child map", func(t *testing.T) {
		db, cleanup := openEmptyDatabase(t)
		defer cleanup()

		err := db.Write(func(tx bolted.WriteTx) error {
			tx.CreateMap(dbpath.ToPath("test"))
			tx.CreateMap(dbpath.ToPath("test", "foo"))
			return nil
		})

		require.NoError(t, err)
		err = db.Write(func(tx bolted.WriteTx) error {
			tx.Delete(dbpath.ToPath("test", "foo"))
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("delete value", func(t *testing.T) {
		db, cleanup := openEmptyDatabase(t)
		defer cleanup()

		err := db.Write(func(tx bolted.WriteTx) error {
			tx.Put(dbpath.ToPath("test"), []byte{1, 2, 3})
			return nil
		})
		require.NoError(t, err)

		err = db.Write(func(tx bolted.WriteTx) error {
			tx.Delete(dbpath.ToPath("test"))
			return nil
		})
		require.NoError(t, err)
	})

}

func TestPutAndGet(t *testing.T) {

	t.Run("put and get to root", func(t *testing.T) {
		db, cleanup := openEmptyDatabase(t)
		defer cleanup()

		err := db.Write(func(tx bolted.WriteTx) error {
			tx.Put(dbpath.ToPath("test"), []byte{1, 2, 3})
			return nil
		})
		require.NoError(t, err)

		var val []byte

		err = db.Read(func(tx bolted.ReadTx) error {
			val = tx.Get(dbpath.ToPath("test"))
			return nil
		})

		require.NoError(t, err)

		require.Equal(t, []byte{1, 2, 3}, val)

	})

	t.Run("put and get to map root", func(t *testing.T) {
		db, cleanup := openEmptyDatabase(t)
		defer cleanup()

		err := db.Write(func(tx bolted.WriteTx) error {
			tx.CreateMap(dbpath.ToPath("test"))
			tx.Put(dbpath.ToPath("test", "foo"), []byte{1, 2, 3})
			return nil
		})
		require.NoError(t, err)

		var val []byte

		err = db.Read(func(tx bolted.ReadTx) error {
			val = tx.Get(dbpath.ToPath("test", "foo"))
			return nil
		})

		require.NoError(t, err)

		require.Equal(t, []byte{1, 2, 3}, val)

		err = db.Read(func(tx bolted.ReadTx) error {
			ex := tx.Exists(dbpath.ToPath("test"))
			require.True(t, ex)

			isMap := tx.IsMap(dbpath.ToPath("test"))
			require.True(t, isMap)

			cnt := tx.Size(dbpath.ToPath("test"))
			require.Equal(t, uint64(1), cnt)

			ex = tx.Exists(dbpath.ToPath("test", "foo"))
			require.True(t, ex)

			isMap = tx.IsMap(dbpath.ToPath("test", "foo"))
			require.False(t, isMap)

			cnt = tx.Size(dbpath.ToPath("test", "foo"))
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
			it := tx.Iterator(dbpath.NilPath)
			require.True(t, it.Done)
			return nil
		})
		require.NoError(t, err)

	})

	t.Run("iterating root with one value", func(t *testing.T) {
		db, cleanup := openEmptyDatabase(t)
		defer cleanup()

		err := db.Write(func(tx bolted.WriteTx) error {
			tx.Put(dbpath.ToPath("test"), []byte{1, 2, 3})
			return nil
		})
		require.NoError(t, err)

		err = db.Read(func(tx bolted.ReadTx) error {
			it := tx.Iterator(dbpath.NilPath)
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
			tx.Put(dbpath.ToPath("test1"), []byte{1, 2, 3})
			tx.Put(dbpath.ToPath("test2"), []byte{2, 3, 4})
			return nil
		})
		require.NoError(t, err)

		err = db.Read(func(tx bolted.ReadTx) error {
			it := tx.Iterator(dbpath.NilPath)

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
			tx.Put(dbpath.ToPath("test1"), []byte{1, 2, 3})
			tx.Put(dbpath.ToPath("test2"), []byte{2, 3, 4})
			tx.CreateMap(dbpath.ToPath("test3"))
			return nil

		})
		require.NoError(t, err)

		err = db.Read(func(tx bolted.ReadTx) error {
			it := tx.Iterator(dbpath.NilPath)

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
