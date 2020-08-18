package watcher_test

import (
	"context"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/draganm/bolted"
	"github.com/draganm/bolted/watcher"
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

type watchEvent struct {
	read bool
	exit error
}

func waitForEvent(t *testing.T, events chan watchEvent, expected watchEvent) {
	select {
	case <-time.NewTimer(2 * time.Second).C:
		require.Fail(t, "timed out waiting for watch to execute")
	case evt := <-events:
		require.Equal(t, expected, evt)
	}
}

func TestWatchPath(t *testing.T) {

	watcher := watcher.New()

	db, cleanupDatabase := openEmptyDatabase(t, bolted.WithChangeListeners(watcher))

	defer cleanupDatabase()

	eventChan := make(chan watchEvent)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		err := watcher.WatchForChanges(ctx, "foo/bar", func(c bolted.ReadTx) error {
			eventChan <- watchEvent{read: true}
			return nil
		})

		eventChan <- watchEvent{
			exit: err,
		}
	}()

	waitForEvent(t, eventChan, watchEvent{read: true})

	err := db.Write(func(tx bolted.WriteTx) error {
		err := tx.CreateMap("foo")
		if err != nil {
			return err
		}
		return tx.Put("foo/bar", []byte{1, 2, 3})
	})

	require.NoError(t, err)

	waitForEvent(t, eventChan, watchEvent{read: true})

	cancel()

	waitForEvent(t, eventChan, watchEvent{exit: context.Canceled})

}

func TestWatchTwoPaths(t *testing.T) {

	watcher := watcher.New()

	db, cleanupDatabase := openEmptyDatabase(t, bolted.WithChangeListeners(watcher))

	defer cleanupDatabase()

	ctx, cancel := context.WithCancel(context.Background())

	eventChan1 := make(chan watchEvent)

	doneErr := errors.New("done")

	go func() {
		cnt := 2
		err := watcher.WatchForChanges(ctx, "foo/bar", func(c bolted.ReadTx) error {
			eventChan1 <- watchEvent{read: true}
			cnt--
			if cnt == 0 {
				return doneErr
			}

			return nil

		})

		eventChan1 <- watchEvent{exit: err}
	}()

	waitForEvent(t, eventChan1, watchEvent{read: true})

	eventChan2 := make(chan watchEvent)

	go func() {
		err := watcher.WatchForChanges(ctx, "foo/bar", func(c bolted.ReadTx) error {
			eventChan2 <- watchEvent{read: true}
			return nil
		})
		eventChan2 <- watchEvent{exit: err}
	}()

	waitForEvent(t, eventChan2, watchEvent{read: true})

	err := db.Write(func(tx bolted.WriteTx) error {
		err := tx.CreateMap("foo")
		if err != nil {
			return err
		}
		return tx.Put("foo/bar", []byte{1, 2, 3})
	})

	require.NoError(t, err)

	waitForEvent(t, eventChan1, watchEvent{read: true})
	waitForEvent(t, eventChan2, watchEvent{read: true})
	waitForEvent(t, eventChan1, watchEvent{exit: doneErr})

	err = db.Write(func(tx bolted.WriteTx) error {
		return tx.Put("foo/bar", []byte{1, 2, 4})
	})

	waitForEvent(t, eventChan2, watchEvent{read: true})

	cancel()

	waitForEvent(t, eventChan2, watchEvent{exit: context.Canceled})

}
