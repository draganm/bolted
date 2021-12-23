package embedded_test

import (
	"testing"
	"time"

	"github.com/draganm/bolted/database"
	"github.com/draganm/bolted/dbpath"
	"github.com/stretchr/testify/require"
)

func TestObservePath(t *testing.T) {

	db, cleanupDatabase := openEmptyDatabase(t)

	defer cleanupDatabase()

	updates, close := db.Observe(dbpath.ToPath("foo").ToMatcher().AppendAnySubpathMatcher())
	defer close()

	t.Run("initial event", func(t *testing.T) {
		initEvent := <-updates
		require.Equal(t, database.ObservedChanges{}, initEvent)
	})

	t.Run("notification of created map", func(t *testing.T) {
		err := database.SugaredWrite(db, func(tx database.SugaredWriteTx) error {
			tx.CreateMap(dbpath.ToPath("foo"))
			return nil
		})
		require.NoError(t, err)

		ev := <-updates

		require.Equal(t, database.ObservedChanges{
			database.ObservedChange{
				Path: dbpath.ToPath("foo"),
				Type: database.ChangeTypeMapCreated,
			},
		}, ev)

	})

	t.Run("notification of set value", func(t *testing.T) {
		err := database.SugaredWrite(db, func(tx database.SugaredWriteTx) error {
			tx.Put(dbpath.ToPath("foo", "bar"), []byte{1, 2, 3})
			return nil
		})
		require.NoError(t, err)

		ev := <-updates

		require.Equal(t, database.ObservedChanges{
			database.ObservedChange{
				Path: dbpath.ToPath("foo", "bar"),
				Type: database.ChangeTypeValueSet,
			},
		}, ev)
	})

	t.Run("notification of deleted value", func(t *testing.T) {
		err := database.SugaredWrite(db, func(tx database.SugaredWriteTx) error {
			tx.Delete(dbpath.ToPath("foo", "bar"))
			return nil
		})
		require.NoError(t, err)

		ev := <-updates

		require.Equal(t, database.ObservedChanges{
			database.ObservedChange{
				Path: dbpath.ToPath("foo", "bar"),
				Type: database.ChangeTypeDeleted,
			},
		}, ev)
	})

	t.Run("notification of storing value and deleting", func(t *testing.T) {
		err := database.SugaredWrite(db, func(tx database.SugaredWriteTx) error {
			tx.Put(dbpath.ToPath("foo", "bar"), []byte{1, 2, 3})
			tx.Delete(dbpath.ToPath("foo"))
			return nil
		})
		require.NoError(t, err)

		ev := <-updates

		require.Equal(t, database.ObservedChanges{
			database.ObservedChange{
				Path: dbpath.ToPath("foo"),
				Type: database.ChangeTypeDeleted,
			},
		}, ev)
	})

	t.Run("no notification sent when unrelated subtree is changed", func(t *testing.T) {
		err := database.SugaredWrite(db, func(tx database.SugaredWriteTx) error {
			tx.CreateMap(dbpath.ToPath("baz"))
			return nil
		})
		require.NoError(t, err)

		select {
		case <-time.Tick(50 * time.Millisecond):
			// expected
		case <-updates:
			require.Fail(t, "unexpected update event")
		}
	})

	t.Run("notification of storing value, deleting and re-creating", func(t *testing.T) {
		err := database.SugaredWrite(db, func(tx database.SugaredWriteTx) error {
			tx.CreateMap(dbpath.ToPath("foo"))
			tx.Put(dbpath.ToPath("foo", "bar"), []byte{1, 2, 3})
			tx.Delete(dbpath.ToPath("foo"))
			tx.CreateMap(dbpath.ToPath("foo"))
			tx.Put(dbpath.ToPath("foo", "bar"), []byte{1, 2, 3})
			return nil
		})
		require.NoError(t, err)

		ev := <-updates

		require.Equal(t, database.ObservedChanges{
			database.ObservedChange{
				Path: dbpath.ToPath("foo"),
				Type: database.ChangeTypeMapCreated,
			},
			database.ObservedChange{
				Path: dbpath.ToPath("foo", "bar"),
				Type: database.ChangeTypeValueSet,
			},
		}, ev)
	})

}
