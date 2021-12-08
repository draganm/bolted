package bolted_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/draganm/bolted"
	"github.com/draganm/bolted/dbpath"
)

type testChangeListener struct {
	openedCalled           bool
	startCalled            bool
	deleteCalled           bool
	createMapCalled        bool
	putCalled              bool
	beforeCommitCalled     bool
	afterTransactionCalled bool
	closedCalled           bool
}

func (c *testChangeListener) Opened(b *bolted.Bolted) error {
	c.openedCalled = true
	return nil
}

func (c *testChangeListener) Start(w bolted.Write) error {
	c.startCalled = true
	return nil
}
func (c *testChangeListener) Delete(w bolted.Write, path dbpath.Path) error {
	c.deleteCalled = true
	return nil
}
func (c *testChangeListener) CreateMap(w bolted.Write, path dbpath.Path) error {
	c.createMapCalled = true
	return nil
}
func (c *testChangeListener) Put(w bolted.Write, path dbpath.Path, newValue []byte) error {
	c.putCalled = true
	return nil
}
func (c *testChangeListener) BeforeCommit(w bolted.Write) error {
	c.beforeCommitCalled = true
	return nil
}
func (c *testChangeListener) AfterTransaction(err error) error {
	c.afterTransactionCalled = true
	return nil
}
func (c *testChangeListener) Closed() error {
	c.closedCalled = true
	return nil
}

func TestChangeListener(t *testing.T) {
	cl := &testChangeListener{}
	bd, cleanup := openEmptyDatabase(t, bolted.WithChangeListeners(cl))

	err := bd.Write(func(tx bolted.Write) error {
		tx.CreateMap(dbpath.ToPath("test"))
		tx.Put(dbpath.ToPath("test", "abc"), []byte{1, 2, 3})
		tx.Delete(dbpath.ToPath("test"))
		return nil
	})

	cleanup()

	require.True(t, cl.openedCalled)
	require.True(t, cl.startCalled)
	require.True(t, cl.beforeCommitCalled)
	require.True(t, cl.afterTransactionCalled)

	require.True(t, cl.deleteCalled)
	require.True(t, cl.createMapCalled)
	require.True(t, cl.putCalled)

	require.True(t, cl.closedCalled)

	require.NoError(t, err)
}
