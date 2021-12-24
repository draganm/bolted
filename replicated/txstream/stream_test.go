package txstream_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/draganm/bolted"
	"github.com/draganm/bolted/dbpath"
	"github.com/draganm/bolted/replicated/txstream"
	"github.com/stretchr/testify/require"
)

type txRecorder struct {
	entries []string
}

func (t *txRecorder) CreateMap(path dbpath.Path) error {
	t.entries = append(t.entries, fmt.Sprintf("CreateMap: %s", path))
	return nil
}

func (t *txRecorder) Delete(path dbpath.Path) error {
	return nil
}

func (t *txRecorder) Put(path dbpath.Path, value []byte) error {
	return nil
}

func (t *txRecorder) Rollback() error {
	return nil
}

func (t *txRecorder) Get(path dbpath.Path) ([]byte, error) {
	return nil, nil
}

func (t *txRecorder) Iterator(path dbpath.Path) (bolted.Iterator, error) {
	return nil, nil
}

func (t *txRecorder) Exists(path dbpath.Path) (bool, error) {
	return false, nil
}

func (t *txRecorder) IsMap(path dbpath.Path) (bool, error) {
	return false, nil
}

func (t *txRecorder) Size(path dbpath.Path) (uint64, error) {
	return 0, nil
}

func (t *txRecorder) Finish() error {
	return nil
}

func TestCreateMap(t *testing.T) {
	buf := new(bytes.Buffer)
	w := txstream.NewWriter(nil, buf)

	err := w.CreateMap(dbpath.ToPath("abc"))
	require.NoError(t, err)

	err = w.Flush()
	require.NoError(t, err)

	rec := &txRecorder{}
	err = txstream.Replay(buf, rec)
	require.NoError(t, err)
	require.Equal(t, []string{"CreateMap: abc"}, rec.entries)

}
