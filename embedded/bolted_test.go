package embedded_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/bolted"
	"github.com/draganm/bolted/dbpath"
	"github.com/draganm/bolted/embedded"
	"github.com/stretchr/testify/require"
)

func openEmptyDatabase(t *testing.T, opts embedded.Options) (bolted.Database, func()) {
	td, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	removeTempDir := func() {
		err = os.RemoveAll(td)
		require.NoError(t, err)
	}

	db, err := embedded.Open(filepath.Join(td, "db"), 0660, opts)

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
	_, cleanup := openEmptyDatabase(t, embedded.Options{})
	defer cleanup()
}

func TestCreateMap(t *testing.T) {

	t.Run("create map", func(t *testing.T) {
		db, cleanup := openEmptyDatabase(t, embedded.Options{})
		defer cleanup()
		err := bolted.SugaredWrite(db, func(tx bolted.SugaredWriteTx) error {
			tx.CreateMap(dbpath.ToPath("test"))
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("create map twice", func(t *testing.T) {
		db, cleanup := openEmptyDatabase(t, embedded.Options{})
		defer cleanup()

		err := bolted.SugaredWrite(db, func(tx bolted.SugaredWriteTx) error {
			tx.CreateMap(dbpath.ToPath("test"))
			return nil
		})
		require.NoError(t, err)

		err = bolted.SugaredWrite(db, func(tx bolted.SugaredWriteTx) error {
			tx.CreateMap(dbpath.ToPath("test"))
			return nil
		})

		require.EqualError(t, err, "CreateMap(test): bucket already exists")
	})

	t.Run("create map nested", func(t *testing.T) {
		db, cleanup := openEmptyDatabase(t, embedded.Options{})
		defer cleanup()

		err := bolted.SugaredWrite(db, func(tx bolted.SugaredWriteTx) error {
			tx.CreateMap(dbpath.ToPath("test"))
			return nil
		})
		require.NoError(t, err)

		err = bolted.SugaredWrite(db, func(tx bolted.SugaredWriteTx) error {
			tx.CreateMap(dbpath.ToPath("test", "foo"))
			return nil
		})
		require.NoError(t, err)

		err = bolted.SugaredRead(db, func(tx bolted.SugaredReadTx) error {
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
		db, cleanup := openEmptyDatabase(t, embedded.Options{})
		defer cleanup()

		err := bolted.SugaredWrite(db, func(tx bolted.SugaredWriteTx) error {
			tx.Delete(dbpath.ToPath("test"))
			return nil
		})
		require.True(t, bolted.IsNotFound(err))
	})

	t.Run("delete existing map", func(t *testing.T) {
		db, cleanup := openEmptyDatabase(t, embedded.Options{})
		defer cleanup()

		err := bolted.SugaredWrite(db, func(tx bolted.SugaredWriteTx) error {
			tx.CreateMap(dbpath.ToPath("test"))
			return nil
		})

		require.NoError(t, err)
		err = bolted.SugaredWrite(db, func(tx bolted.SugaredWriteTx) error {
			tx.Delete(dbpath.ToPath("test"))
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("delete parent map", func(t *testing.T) {
		db, cleanup := openEmptyDatabase(t, embedded.Options{})
		defer cleanup()

		err := bolted.SugaredWrite(db, func(tx bolted.SugaredWriteTx) error {
			tx.CreateMap(dbpath.ToPath("test"))
			tx.CreateMap(dbpath.ToPath("test", "foo"))
			return nil
		})

		require.NoError(t, err)
		err = bolted.SugaredWrite(db, func(tx bolted.SugaredWriteTx) error {
			tx.Delete(dbpath.ToPath("test"))
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("delete child map", func(t *testing.T) {
		db, cleanup := openEmptyDatabase(t, embedded.Options{})
		defer cleanup()

		err := bolted.SugaredWrite(db, func(tx bolted.SugaredWriteTx) error {
			tx.CreateMap(dbpath.ToPath("test"))
			tx.CreateMap(dbpath.ToPath("test", "foo"))
			return nil
		})

		require.NoError(t, err)
		err = bolted.SugaredWrite(db, func(tx bolted.SugaredWriteTx) error {
			tx.Delete(dbpath.ToPath("test", "foo"))
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("delete value", func(t *testing.T) {
		db, cleanup := openEmptyDatabase(t, embedded.Options{})
		defer cleanup()

		err := bolted.SugaredWrite(db, func(tx bolted.SugaredWriteTx) error {
			tx.Put(dbpath.ToPath("test"), []byte{1, 2, 3})
			return nil
		})
		require.NoError(t, err)

		err = bolted.SugaredWrite(db, func(tx bolted.SugaredWriteTx) error {
			tx.Delete(dbpath.ToPath("test"))
			return nil
		})
		require.NoError(t, err)
	})

}

func TestPutAndGet(t *testing.T) {

	t.Run("put and get to root", func(t *testing.T) {
		db, cleanup := openEmptyDatabase(t, embedded.Options{})
		defer cleanup()

		err := bolted.SugaredWrite(db, func(tx bolted.SugaredWriteTx) error {
			tx.Put(dbpath.ToPath("test"), []byte{1, 2, 3})
			return nil
		})
		require.NoError(t, err)

		var val []byte

		err = bolted.SugaredRead(db, func(tx bolted.SugaredReadTx) error {
			val = tx.Get(dbpath.ToPath("test"))
			return nil
		})

		require.NoError(t, err)

		require.Equal(t, []byte{1, 2, 3}, val)

	})

	t.Run("put and get to map root", func(t *testing.T) {
		db, cleanup := openEmptyDatabase(t, embedded.Options{})
		defer cleanup()

		err := bolted.SugaredWrite(db, func(tx bolted.SugaredWriteTx) error {
			tx.CreateMap(dbpath.ToPath("test"))
			tx.Put(dbpath.ToPath("test", "foo"), []byte{1, 2, 3})
			return nil
		})
		require.NoError(t, err)

		var val []byte

		err = bolted.SugaredRead(db, func(tx bolted.SugaredReadTx) error {
			val = tx.Get(dbpath.ToPath("test", "foo"))
			return nil
		})

		require.NoError(t, err)

		require.Equal(t, []byte{1, 2, 3}, val)

		err = bolted.SugaredRead(db, func(tx bolted.SugaredReadTx) error {
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
		db, cleanup := openEmptyDatabase(t, embedded.Options{})
		defer cleanup()

		err := bolted.SugaredWrite(db, func(tx bolted.SugaredWriteTx) error {
			it := tx.Iterator(dbpath.NilPath)
			require.True(t, it.IsDone())
			return nil
		})
		require.NoError(t, err)

	})

	t.Run("iterating root with one value", func(t *testing.T) {
		db, cleanup := openEmptyDatabase(t, embedded.Options{})
		defer cleanup()

		err := bolted.SugaredWrite(db, func(tx bolted.SugaredWriteTx) error {
			tx.Put(dbpath.ToPath("test"), []byte{1, 2, 3})
			return nil
		})
		require.NoError(t, err)

		err = bolted.SugaredRead(db, func(tx bolted.SugaredReadTx) error {
			it := tx.Iterator(dbpath.NilPath)
			require.False(t, it.IsDone())

			require.Equal(t, "test", it.GetKey())
			require.Equal(t, []byte{1, 2, 3}, it.GetValue())

			it.Next()
			require.True(t, it.IsDone())

			return nil
		})
		require.NoError(t, err)

	})

	t.Run("iterating root with two values", func(t *testing.T) {
		db, cleanup := openEmptyDatabase(t, embedded.Options{})
		defer cleanup()

		err := bolted.SugaredWrite(db, func(tx bolted.SugaredWriteTx) error {
			tx.Put(dbpath.ToPath("test1"), []byte{1, 2, 3})
			tx.Put(dbpath.ToPath("test2"), []byte{2, 3, 4})
			return nil
		})
		require.NoError(t, err)

		err = bolted.SugaredRead(db, func(tx bolted.SugaredReadTx) error {
			it := tx.Iterator(dbpath.NilPath)

			require.False(t, it.IsDone())
			require.Equal(t, "test1", it.GetKey())
			require.Equal(t, []byte{1, 2, 3}, it.GetValue())

			it.Next()

			require.False(t, it.IsDone())
			require.Equal(t, "test2", it.GetKey())
			require.Equal(t, []byte{2, 3, 4}, it.GetValue())

			it.Prev()

			require.False(t, it.IsDone())
			require.Equal(t, "test1", it.GetKey())
			require.Equal(t, []byte{1, 2, 3}, it.GetValue())

			it.Last()

			require.False(t, it.IsDone())
			require.Equal(t, "test2", it.GetKey())
			require.Equal(t, []byte{2, 3, 4}, it.GetValue())

			it.Next()

			require.True(t, it.IsDone())

			return nil
		})
		require.NoError(t, err)

	})

	t.Run("iterating root with two values and a bucket", func(t *testing.T) {
		db, cleanup := openEmptyDatabase(t, embedded.Options{})
		defer cleanup()

		err := bolted.SugaredWrite(db, func(tx bolted.SugaredWriteTx) error {
			tx.Put(dbpath.ToPath("test1"), []byte{1, 2, 3})
			tx.Put(dbpath.ToPath("test2"), []byte{2, 3, 4})
			tx.CreateMap(dbpath.ToPath("test3"))
			return nil

		})
		require.NoError(t, err)

		err = bolted.SugaredRead(db, func(tx bolted.SugaredReadTx) error {
			it := tx.Iterator(dbpath.NilPath)

			require.False(t, it.IsDone())
			require.Equal(t, "test1", it.GetKey())
			require.Equal(t, []byte{1, 2, 3}, it.GetValue())

			it.Next()

			require.False(t, it.IsDone())
			require.Equal(t, "test2", it.GetKey())
			require.Equal(t, []byte{2, 3, 4}, it.GetValue())

			it.Next()

			require.False(t, it.IsDone())
			require.Equal(t, "test3", it.GetKey())
			require.Equal(t, []byte(nil), it.GetValue())

			it.Next()

			require.True(t, it.IsDone())

			return nil
		})
		require.NoError(t, err)

	})

}

func TestSize(t *testing.T) {

	testCases := []struct {
		name         string
		tx           func(tx bolted.SugaredWriteTx) error
		path         dbpath.Path
		expectedSize uint64
	}{
		{
			"empty root",
			func(tx bolted.SugaredWriteTx) error {
				return nil
			},
			dbpath.NilPath,
			0,
		},
		{
			"one map in root",
			func(tx bolted.SugaredWriteTx) error {
				tx.CreateMap(dbpath.ToPath("foo"))
				return nil
			},
			dbpath.NilPath,
			1,
		},
		{
			"one value in root",
			func(tx bolted.SugaredWriteTx) error {
				tx.Put(dbpath.ToPath("foo"), []byte{})
				return nil
			},
			dbpath.NilPath,
			1,
		},
		{
			"two values in root",
			func(tx bolted.SugaredWriteTx) error {
				tx.Put(dbpath.ToPath("foo"), []byte{})
				tx.Put(dbpath.ToPath("bar"), []byte{})
				return nil
			},
			dbpath.NilPath,
			2,
		},
		{
			"two maps in root",
			func(tx bolted.SugaredWriteTx) error {
				tx.CreateMap(dbpath.ToPath("foo"))
				tx.CreateMap(dbpath.ToPath("bar"))
				return nil
			},
			dbpath.NilPath,
			2,
		},
		{
			"nested maps in root",
			func(tx bolted.SugaredWriteTx) error {
				tx.CreateMap(dbpath.ToPath("foo"))
				tx.CreateMap(dbpath.ToPath("foo", "bar"))
				return nil
			},
			dbpath.NilPath,
			1,
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			db, cleanup := openEmptyDatabase(t, embedded.Options{})
			defer cleanup()

			var sz uint64
			err := bolted.SugaredWrite(db, func(tx bolted.SugaredWriteTx) error {
				err := tc.tx(tx)
				if err != nil {
					return err
				}
				sz = tx.Size(tc.path)
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, tc.expectedSize, sz)

			err = bolted.SugaredRead(db, func(tx bolted.SugaredReadTx) error {
				sz = tx.Size(tc.path)
				return nil
			})

			require.NoError(t, err)

			require.Equal(t, tc.expectedSize, sz, "after read tx")

		})
	}

}
