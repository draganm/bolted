package wal_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/bolted/replicated/primary/server/wal"
	"github.com/stretchr/testify/require"
)

func TestWalAppendToEmpty(t *testing.T) {
	require := require.New(t)
	td, err := os.MkdirTemp("", "*")
	require.NoError(err)

	t.Cleanup(func() {
		os.RemoveAll(td)
	})

	w, err := wal.Open(filepath.Join(td, "mywal"))
	require.NoError(err)

	t.Cleanup(func() {
		w.Close()
	})

	bb := &bytes.Buffer{}
	bb.Write([]byte{1, 2, 3})
	err = w.Append(2, bb)
	require.NoError(err)

	res := new(bytes.Buffer)
	err = w.CopyOrWait(context.Background(), 2, res)
	require.NoError(err)

	require.Equal([]byte{1, 2, 3}, res.Bytes())

}
