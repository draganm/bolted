package features

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/bolted"
	"github.com/draganm/bolted/dbpath"
	"github.com/draganm/senfgurke/step"
	"github.com/draganm/senfgurke/testrunner"
	"github.com/draganm/senfgurke/world"
)

func TestFeatures(t *testing.T) {
	testrunner.RunScenarios(t, steps)
}

var steps = step.NewRegistry()

var _ = steps.Then("the database is open", func(w *world.World) error {
	td, err := os.MkdirTemp("", "*")
	if err != nil {
		return fmt.Errorf("while creating temp dir: %w", err)
	}

	w.AddCleanup(func() error {
		return os.RemoveAll(td)
	})

	db, err := bolted.Open(filepath.Join(td, "db"), 0700)
	if err != nil {
		return err
	}

	w.Attributes["db"] = db
	w.AddCleanup(db.Close)

	return nil
})

func getDB(w *world.World) *bolted.Bolted {
	return w.Attributes["db"].(*bolted.Bolted)
}

var _ = steps.Then("I create a map {string}", func(w *world.World, mapName string) error {
	db := getDB(w)
	return db.Write(func(tx bolted.WriteTx) error {
		tx.CreateMap(dbpath.ToPath(mapName))
		return nil
	})
})

var _ = steps.Then("the map {string} should exist", func(w *world.World, mapName string) error {
	db := getDB(w)
	return db.Read(func(tx bolted.ReadTx) error {
		w.Assert.True(tx.Exists(dbpath.ToPath(mapName)))
		return nil
	})
})

var _ = steps.Then("the map {string} should be empty", func(w *world.World, mapName string) error {
	db := getDB(w)
	return db.Read(func(tx bolted.ReadTx) error {
		w.Assert.Equal(uint64(0), tx.Size(dbpath.ToPath(mapName)))
		return nil
	})
})

var _ = steps.Then("I have created a map {string}", func(w *world.World, mapName string) error {
	db := getDB(w)
	return db.Write(func(tx bolted.WriteTx) error {
		tx.CreateMap(dbpath.ToPath(mapName))
		return nil
	})
})

var _ = steps.Then("I delete the map {string}", func(w *world.World, mapName string) error {
	db := getDB(w)
	return db.Write(func(tx bolted.WriteTx) error {
		tx.Delete(dbpath.ToPath(mapName))
		return nil
	})
})

var _ = steps.Then("the map {string} should not exist", func(w *world.World, mapName string) error {
	db := getDB(w)
	return db.Write(func(tx bolted.WriteTx) error {
		w.Assert.False(tx.Exists(dbpath.ToPath(mapName)))
		return nil
	})
})

var _ = steps.Then("the root should have {int} element", func(w *world.World, expected int) error {
	db := getDB(w)
	return db.Read(func(tx bolted.ReadTx) error {
		w.Assert.Equal(uint64(expected), tx.Size(dbpath.NilPath))
		return nil
	})
})

var _ = steps.Then("the root should have {int} elements", func(w *world.World, expected int) error {
	db := getDB(w)
	return db.Read(func(tx bolted.ReadTx) error {
		w.Assert.Equal(uint64(expected), tx.Size(dbpath.NilPath))
		return nil
	})
})
