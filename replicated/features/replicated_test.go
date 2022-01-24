package features

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/draganm/bolted"
	"github.com/draganm/bolted/dbpath"
	"github.com/draganm/bolted/replicated/primary/server"
	"github.com/draganm/bolted/replicated/replica"
	"github.com/draganm/senfgurke/step"
	"github.com/draganm/senfgurke/testrunner"
	"github.com/draganm/senfgurke/world"
)

func TestFeatures(t *testing.T) {
	testrunner.RunScenarios(t, steps)
}

var steps = step.NewRegistry()

func startEmptyPrimaryServer(w *world.World) error {
	td, err := os.MkdirTemp("", "*")
	if err != nil {
		return fmt.Errorf("while creating temp dir: %w", err)
	}

	w.AddCleanup(func() error {
		return os.RemoveAll(td)
	})

	ps, err := server.New(filepath.Join(td, "source"))
	if err != nil {
		return fmt.Errorf("while starting server: %w", err)
	}

	l, err := net.Listen("tcp", ":0")
	if err != nil {
		return fmt.Errorf("while starting listener: %w", err)
	}

	s := &http.Server{
		Handler: ps,
	}

	go s.Serve(l)

	w.AddCleanup(func() error {
		return s.Close()
	})

	w.Put("primaryURL", fmt.Sprintf("http://%s/db", l.Addr().String()))
	return nil

}

var _ = steps.Given("an empty primary server", func(w *world.World) error {
	return startEmptyPrimaryServer(w)
})

func runInTemporaryReplica(w *world.World, wtx func(tx bolted.SugaredWriteTx) error) error {
	td, err := os.MkdirTemp("", "*")

	if err != nil {
		return err
	}

	defer os.RemoveAll(td)

	replica, err := replica.Open(context.Background(), w.GetString("primaryURL"), filepath.Join(td, "db"))
	if err != nil {
		return err
	}

	defer replica.Close()

	return bolted.SugaredWrite(replica, wtx)

}

func openReplica(w *world.World) error {
	td, err := os.MkdirTemp("", "*")
	if err != nil {
		return fmt.Errorf("while creating temp dir: %w", err)
	}

	w.AddCleanup(func() error {
		return os.RemoveAll(td)
	})

	replica, err := replica.Open(context.Background(), w.GetString("primaryURL"), filepath.Join(td, "db"))
	if err == nil {
		w.AddCleanup(replica.Close)
	}

	w.Put("lastError", err)
	w.Put("replica", replica)
	return nil
}

var _ = steps.Then("I connect to the primary server", func(w *world.World) error {
	return openReplica(w)

})

var _ = steps.Then("the connecting should succeed", func(w *world.World) {
	w.Require.Nil(w.Attributes["lastError"])
})

var _ = steps.When("a replica connected to the primary", func(w *world.World) error {
	return openReplica(w)
})

var _ = steps.Then("I execute putting of {string} on the path {string} on the replica", func(w *world.World, value string, pathString string) error {
	replica := w.Attributes["replica"].(bolted.Database)
	return bolted.SugaredWrite(replica, func(tx bolted.SugaredWriteTx) error {
		tx.Put(dbpath.MustParse(pathString), []byte(value))
		return nil
	})
})

var _ = steps.Then("the primary server should have value {string} on the path {string}", func(w *world.World, value string, pathString string) error {
	primaryDB := w.Attributes["primaryDB"].(bolted.Database)
	return bolted.SugaredRead(primaryDB, func(tx bolted.SugaredReadTx) error {
		w.Assert.Equal([]byte(value), tx.Get(dbpath.MustParse(pathString)))
		return nil
	})
})

var _ = steps.Then("the replica should have value {string} on the path {string}", func(w *world.World, value string, pathString string) error {
	replica := w.Attributes["replica"].(bolted.Database)
	return bolted.SugaredRead(replica, func(tx bolted.SugaredReadTx) error {
		w.Assert.Equal([]byte(value), tx.Get(dbpath.MustParse(pathString)))
		return nil
	})
})

var _ = steps.Given("an running primary server", func(w *world.World) error {
	return startEmptyPrimaryServer(w)
})

var _ = steps.Given("value of {string} being set ot {string} on the primary server", func(w *world.World, pathString string, value string) error {
	return runInTemporaryReplica(w, func(tx bolted.SugaredWriteTx) error {
		tx.Put(dbpath.MustParse(pathString), []byte(value))
		return nil
	})
})

var _ = steps.Then("the value of {string} should be {string} on the replica", func(w *world.World, pathString string, expectedValue string) error {
	primaryDB := w.Attributes["replica"].(bolted.Database)
	time.Sleep(100 * time.Millisecond)
	return bolted.SugaredRead(primaryDB, func(tx bolted.SugaredReadTx) error {
		w.Assert.Equal(expectedValue, string(tx.Get(dbpath.MustParse(pathString))))
		return nil
	})
})
