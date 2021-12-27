package features

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/bolted"
	"github.com/draganm/bolted/dbpath"
	"github.com/draganm/bolted/embedded"
	"github.com/draganm/bolted/replicated"
	"github.com/draganm/bolted/replicated/txstream"
	"github.com/draganm/senfgurke/step"
	"github.com/draganm/senfgurke/testrunner"
	"github.com/draganm/senfgurke/world"
)

func TestFeatures(t *testing.T) {
	testrunner.RunScenarios(t, steps)
}

var steps = step.NewRegistry()

var _ = steps.Then("empty source and destination database", func(w *world.World) error {
	td, err := os.MkdirTemp("", "*")
	if err != nil {
		return fmt.Errorf("while creating temp dir: %w", err)
	}

	w.AddCleanup(func() error {
		return os.RemoveAll(td)
	})

	source, err := embedded.Open(filepath.Join(td, "source"), 0700)
	if err != nil {
		return fmt.Errorf("while opening source db: %w", err)
	}

	w.Put("source", newMockReplica(source))
	w.Put("sourceOriginal", source)

	w.AddCleanup(source.Close)

	destination, err := embedded.Open(filepath.Join(td, "destination"), 0700)
	if err != nil {
		return fmt.Errorf("while opening destination db: %w", err)
	}

	w.AddCleanup(destination.Close)

	w.Put("destination", destination)

	return nil
})

func replicateTransaction(w *world.World, tx func(tx bolted.SugaredWriteTx) error) error {

	sourceDb := w.Attributes["source"].(mockReplica)
	err := bolted.SugaredWrite(sourceDb, tx)

	if err != nil {
		return fmt.Errorf("while running source tx: %w", err)
	}

	destinationDb := w.Attributes["destination"].(bolted.Database)

	err = txstream.Replay(bytes.NewReader(sourceDb.txLog.Bytes()), destinationDb)
	if err != nil {
		return fmt.Errorf("while replaying transaction to destination: %w", err)
	}

	err = txstream.Replay(bytes.NewReader(sourceDb.txLog.Bytes()), w.Attributes["sourceOriginal"].(bolted.Database))
	if err != nil {
		return fmt.Errorf("while replaying transaction to original source: %w", err)
	}

	sourceDb.txLog.Reset()

	return nil

}

var _ = steps.When("I replicate creating a new map with path {string}", func(w *world.World, pathString string) error {

	return replicateTransaction(w, func(tx bolted.SugaredWriteTx) error {
		tx.CreateMap(dbpath.MustParse(pathString))
		return nil
	})

})

func destinationReadTransaction(w *world.World, tx func(tx bolted.SugaredReadTx) error) error {
	destinationDb := w.Attributes["destination"].(bolted.Database)
	return bolted.SugaredRead(destinationDb, tx)
}

func destinationWriteTransaction(w *world.World, tx func(tx bolted.SugaredWriteTx) error) error {
	destinationDb := w.Attributes["destination"].(bolted.Database)
	return bolted.SugaredWrite(destinationDb, tx)
}

var _ = steps.Then("the destination database should have a map with path {string}", func(w *world.World, pathString string) error {
	return destinationReadTransaction(w, func(tx bolted.SugaredReadTx) error {
		w.Assert.True(tx.IsMap(dbpath.MustParse(pathString)))
		return nil
	})
})

var _ = steps.Then("a replicated map with path {string}", func(w *world.World, p string) error {
	return replicateTransaction(w, func(tx bolted.SugaredWriteTx) error {
		tx.CreateMap(dbpath.MustParse(p))
		return nil
	})
})

var _ = steps.Then("the path {string} should not exist in the destination database", func(w *world.World, pathString string) error {
	return destinationReadTransaction(w, func(tx bolted.SugaredReadTx) error {
		w.Assert.False(tx.Exists(dbpath.MustParse(pathString)))
		return nil
	})

})

var _ = steps.Then("I replicate deleting path {string}", func(w *world.World, pathString string) error {
	return replicateTransaction(w, func(tx bolted.SugaredWriteTx) error {
		tx.Delete(dbpath.MustParse(pathString))
		return nil
	})
})

var _ = steps.Then("I try to delete a map {string}", func(w *world.World, pathString string) {
	err := replicateTransaction(w, func(tx bolted.SugaredWriteTx) error {
		tx.Delete(dbpath.MustParse(pathString))
		return nil
	})

	w.Put("lastError", err)
})

var _ = steps.Then("NotExisting error should be returned in the original transaction", func(w *world.World) {
	w.Require.ErrorIs(w.Attributes["lastError"].(error), bolted.ErrNotFound)
})

var _ = steps.Then("I replicate putting {string} into path {string}", func(w *world.World, data string, pathString string) error {
	return replicateTransaction(w, func(tx bolted.SugaredWriteTx) error {
		tx.Put(dbpath.MustParse(pathString), []byte(data))
		return nil
	})
})

var _ = steps.Then("the destination database should have a value with path {string}", func(w *world.World, pathString string) error {
	return destinationReadTransaction(w, func(tx bolted.SugaredReadTx) error {
		w.Assert.True(tx.Exists(dbpath.MustParse(pathString)))
		return nil
	})
})

var _ = steps.Then("I try putting {string} into path {string}", func(w *world.World, data string, pathString string) {
	err := replicateTransaction(w, func(tx bolted.SugaredWriteTx) error {
		tx.Put(dbpath.MustParse(pathString), []byte(data))
		return nil
	})

	w.Put("lastError", err)
})

var _ = steps.Then("Conflict error should be returned in the original transaction", func(w *world.World) {
	w.Require.ErrorIs(w.Attributes["lastError"].(error), bolted.ErrConflict)
})

var _ = steps.Then("the value of {string} has changed to {string} in the destination database", func(w *world.World, pathString string, newValue string) error {
	return destinationWriteTransaction(w, func(tx bolted.SugaredWriteTx) error {
		tx.Put(dbpath.MustParse(pathString), []byte(newValue))
		return nil
	})
})

var _ = steps.Then("I try to replicate getting the value {string}", func(w *world.World, pathString string) {
	err := replicateTransaction(w, func(tx bolted.SugaredWriteTx) error {
		_ = tx.Get(dbpath.MustParse(pathString))
		return nil
	})
	w.Put("lastError", err)
})

var _ = steps.Then("Stale error should be returned on replication", func(w *world.World) {
	w.Require.ErrorIs(w.Attributes["lastError"].(error), replicated.ErrStale)
})

var _ = steps.Then("the I put {string} into path {string} in the destination database", func(w *world.World, data string, pathString string) error {
	return destinationWriteTransaction(w, func(tx bolted.SugaredWriteTx) error {
		tx.Put(dbpath.MustParse(pathString), []byte(data))
		return nil
	})
})

var _ = steps.Then("I try replicating geting the size of root", func(w *world.World) {
	err := replicateTransaction(w, func(tx bolted.SugaredWriteTx) error {
		_ = tx.Size(dbpath.NilPath)
		return nil
	})
	w.Put("lastError", err)
})

var _ = steps.Then("I replicate iteration over the root", func(w *world.World) error {
	return replicateTransaction(w, func(tx bolted.SugaredWriteTx) error {
		for it := tx.Iterator(dbpath.NilPath); !it.IsDone(); it.Next() {
			// do nothing
		}
		return nil
	})
})

var _ = steps.Then("the replication should succeed", func(w *world.World) {
	// this is a no-op, since the previous step would fail if replication fails
})

// ----------

func newMockReplica(db bolted.Database) mockReplica {
	return mockReplica{
		Database: db,
		txLog:    new(bytes.Buffer),
	}
}

type mockReplica struct {
	bolted.Database
	txLog *bytes.Buffer
}

func (m mockReplica) BeginWrite() (bolted.WriteTx, error) {
	rtx, err := m.BeginRead()
	if err != nil {
		return nil, err
	}

	return txstream.NewWriter(rtx, m.txLog), nil
}
