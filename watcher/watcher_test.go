package watcher_test

import (
	"context"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
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

func TestWatchPath(t *testing.T) {

	watcher := watcher.New()

	db, cleanupDatabase := openEmptyDatabase(t, bolted.WithChangeListeners(watcher))

	defer cleanupDatabase()

	errchan := make(chan error)

	readchan := make(chan bool)

	ctx, cancel := context.WithCancel(context.Background())

	wg := new(sync.WaitGroup)

	wg.Add(1)

	go func() {
		wg.Done()
		errchan <- watcher.WatchForChanges(ctx, "foo/bar", func(c bolted.ReadTx) error {
			readchan <- true
			return nil
		})
	}()

	wg.Wait()

	err := db.Write(func(tx bolted.WriteTx) error {
		err := tx.CreateMap("foo")
		if err != nil {
			return err
		}
		return tx.Put("foo/bar", []byte{1, 2, 3})
	})

	require.NoError(t, err)

	select {
	case <-time.NewTimer(2 * time.Second).C:
		require.Fail(t, "timed out waiting for watch to execute")
	case <-readchan:
	}

	cancel()

	select {
	case <-time.NewTimer(time.Second).C:
		require.Fail(t, "timed out waiting for watch to terminate")
	case err = <-errchan:
		require.Equal(t, context.Canceled, err)
	}

}

func TestWatchTwoPaths(t *testing.T) {

	watcher := watcher.New()

	db, cleanupDatabase := openEmptyDatabase(t, bolted.WithChangeListeners(watcher))

	defer cleanupDatabase()

	ctx, cancel := context.WithCancel(context.Background())

	wg := new(sync.WaitGroup)

	wg.Add(2)

	errchan1 := make(chan error)

	readchan1 := make(chan bool)

	doneErr := errors.New("done")

	go func() {
		wg.Done()
		errchan1 <- watcher.WatchForChanges(ctx, "foo/bar", func(c bolted.ReadTx) error {
			readchan1 <- true
			return doneErr
		})
	}()

	errchan2 := make(chan error)

	readchan2 := make(chan bool)

	go func() {
		wg.Done()
		errchan2 <- watcher.WatchForChanges(ctx, "foo/bar", func(c bolted.ReadTx) error {
			readchan2 <- true
			return nil
		})
	}()

	wg.Wait()

	err := db.Write(func(tx bolted.WriteTx) error {
		err := tx.CreateMap("foo")
		if err != nil {
			return err
		}
		return tx.Put("foo/bar", []byte{1, 2, 3})
	})

	require.NoError(t, err)

	select {
	case <-time.NewTimer(1 * time.Second).C:
		require.Fail(t, "timed out waiting for watch to execute")
	case <-readchan1:
	}

	select {
	case <-time.NewTimer(1 * time.Second).C:
		require.Fail(t, "timed out waiting for watch to execute")
	case <-readchan2:
	}

	select {
	case <-time.NewTimer(time.Second).C:
		require.Fail(t, "timed out waiting for watch to terminate")
	case err = <-errchan1:
		require.Equal(t, doneErr, err)
	}

	err = db.Write(func(tx bolted.WriteTx) error {
		return tx.Put("foo/bar", []byte{1, 2, 4})
	})

	select {
	case <-time.NewTimer(1 * time.Second).C:
		require.Fail(t, "timed out waiting for watch to execute")
	case <-readchan2:
	}

	cancel()

	select {
	case <-time.NewTimer(time.Second).C:
		require.Fail(t, "timed out waiting for watch to terminate")
	case err = <-errchan2:
		require.Equal(t, context.Canceled, err)
	}

}
