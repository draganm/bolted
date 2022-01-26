package flexbuffer_test

import (
	"io/ioutil"
	"testing"

	"github.com/draganm/bolted/util/flexbuffer"
	"github.com/stretchr/testify/require"
)

func TestFlexBufferHead(t *testing.T) {
	fb := flexbuffer.New(5)

	require := require.New(t)

	n, err := fb.Write([]byte{1, 2, 3, 4, 5})
	require.NoError(err)
	require.Equal(5, n)

	err = fb.FinishWrite()
	require.NoError(err)

	r, err := ioutil.ReadAll(fb)
	require.NoError(err)
	require.Equal([]byte{1, 2, 3, 4, 5}, r)

}

func TestFlexBufferHeadAndTail(t *testing.T) {
	fb := flexbuffer.New(5)

	require := require.New(t)

	n, err := fb.Write([]byte{1, 2, 3, 4, 5, 6, 7})
	require.NoError(err)
	require.Equal(7, n)

	err = fb.FinishWrite()
	require.NoError(err)

	r, err := ioutil.ReadAll(fb)
	require.NoError(err)
	require.Equal([]byte{1, 2, 3, 4, 5, 6, 7}, r)

}
