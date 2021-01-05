package dump_test

import (
	"bytes"
	"testing"

	"github.com/draganm/bolted/dump"
	"github.com/stretchr/testify/require"
)

func TestWrite(t *testing.T) {
	bb := new(bytes.Buffer)

	wr := dump.NewWriter(bb)
	n, err := wr.CreateMap("abc")
	require.NoError(t, err)

	require.Equal(t, 5, n)
	require.Equal(t, []byte{1, 3, 'a', 'b', 'c'}, bb.Bytes())

	n, err = wr.Put("abc/def", []byte{1, 2, 3})
	require.NoError(t, err)

	require.Equal(t, 13, n)
	require.Equal(t, []byte{1, 3, 'a', 'b', 'c', 2, 7, 'a', 'b', 'c', '/', 'd', 'e', 'f', 3, 1, 2, 3}, bb.Bytes())

}
