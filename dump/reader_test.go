package dump_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/draganm/bolted/dump"
	"github.com/stretchr/testify/require"
)

func TestReader(t *testing.T) {
	bb := new(bytes.Buffer)
	dw := dump.NewWriter(bb)

	_, err := dw.CreateMap("foo")
	require.NoError(t, err)

	_, err = dw.CreateMap("foo/bar")

	_, err = dw.Put("foo/bar/baz", []byte{1, 2, 3})
	require.NoError(t, err)

	dr := dump.NewReader(bb)

	it, err := dr.Next()
	require.NoError(t, err)

	require.Equal(t, dump.Item{Type: dump.CreateMap, Key: "foo"}, it)

	it, err = dr.Next()
	require.NoError(t, err)

	require.Equal(t, dump.Item{Type: dump.CreateMap, Key: "foo/bar"}, it)

	it, err = dr.Next()
	require.NoError(t, err)

	require.Equal(t, dump.Item{Type: dump.Put, Key: "foo/bar/baz", Value: []byte{1, 2, 3}}, it)

	_, err = dr.Next()
	require.Same(t, io.EOF, err)

}
