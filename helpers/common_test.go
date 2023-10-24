package helpers_test

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/bolted"
	"github.com/stretchr/testify/require"
)

var ctx = context.Background()

func openEmptyDatabase(t *testing.T, opts bolted.Options) (bolted.Database, func()) {
	td, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	removeTempDir := func() {
		err = os.RemoveAll(td)
		require.NoError(t, err)
	}

	bdb, err := bolted.Open(filepath.Join(td, "db"), 0660, opts)

	require.NoError(t, err)

	closeDatabase := func() {
		err = bdb.Close()
		require.NoError(t, err)
	}

	return bdb, func() {
		closeDatabase()
		removeTempDir()
	}

}
