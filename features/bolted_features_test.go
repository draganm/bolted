package features

import (
	"context"
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
var ctx = context.Background()

var _ = steps.Then("the database is open", func(w *world.World) error {
	td, err := os.MkdirTemp("", "*")
	if err != nil {
		return fmt.Errorf("while creating temp dir: %w", err)
	}

	w.AddCleanup(func() error {
		return os.RemoveAll(td)
	})

	db, err := bolted.Open(filepath.Join(td, "db"), 0700, bolted.Options{})
	if err != nil {
		return err
	}

	w.Attributes["db"] = db
	w.AddCleanup(db.Close)

	return nil
})

func getDB(w *world.World) bolted.Database {
	return w.Attributes["db"].(bolted.Database)
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
		w.Assert.Equal(uint64(0), tx.GetSizeOf(dbpath.ToPath(mapName)))
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
		w.Assert.Equal(uint64(expected), tx.GetSizeOf(dbpath.NilPath))
		return nil
	})
})

var _ = steps.Then("the root should have {int} elements", func(w *world.World, expected int) error {
	db := getDB(w)
	return db.Read(func(tx bolted.ReadTx) error {
		w.Assert.Equal(uint64(expected), tx.GetSizeOf(dbpath.NilPath))
		return nil
	})
})

var _ = steps.Then("I put {string} data under {string} in the root", func(w *world.World, content string, dataName string) error {
	db := getDB(w)
	return db.Write(func(tx bolted.WriteTx) error {
		tx.Put(dbpath.ToPath(dataName), []byte(content))
		return nil
	})
})

var _ = steps.Then("the data {string} should exist", func(w *world.World, dataName string) error {
	db := getDB(w)
	return db.Read(func(tx bolted.ReadTx) error {
		w.Assert.True(tx.Exists(dbpath.ToPath(dataName)))
		return nil
	})
})

var _ = steps.Then("the context of the data {string} should be {string}", func(w *world.World, dataName string, expectedContent string) error {
	db := getDB(w)
	return db.Read(func(tx bolted.ReadTx) error {
		w.Assert.Equal(expectedContent, string(tx.Get(dbpath.ToPath(dataName))))
		return nil
	})
})

var _ = steps.Then("there is data with name {string} in the root", func(w *world.World, dataName string) error {
	db := getDB(w)
	return db.Write(func(tx bolted.WriteTx) error {
		tx.Put(dbpath.ToPath(dataName), []byte("this is a test"))
		return nil
	})
})

var _ = steps.Then("I delete data {string} from the root", func(w *world.World, dataName string) error {
	db := getDB(w)
	return db.Write(func(tx bolted.WriteTx) error {
		tx.Delete(dbpath.ToPath(dataName))
		return nil
	})
})

var _ = steps.Then("the data {string} should not exist", func(w *world.World, dataName string) error {
	db := getDB(w)
	return db.Read(func(tx bolted.ReadTx) error {
		w.Assert.False(tx.Exists(dbpath.ToPath(dataName)))
		return nil
	})
})

var _ = steps.Then("there are {int} maps and {int} data entries in the root", func(w *world.World, countMaps int, countData int) error {
	db := getDB(w)
	return db.Write(func(tx bolted.WriteTx) error {
		cnt := 0
		for i := 0; i < countMaps; i++ {
			tx.CreateMap(dbpath.ToPath(fmt.Sprintf("%02d", cnt)))
			cnt++
		}
		for i := 0; i < countData; i++ {
			tx.Put(dbpath.ToPath(fmt.Sprintf("%02d", cnt)), []byte(fmt.Sprintf("%d", i)))
			cnt++
		}
		return nil
	})
})

var _ = steps.Then("I iterate over all entries", func(w *world.World) {
	db := getDB(w)
	result := [][2]string{}
	err := db.Read(func(tx bolted.ReadTx) error {
		for it := tx.Iterate(dbpath.NilPath); !it.IsDone(); it.Next() {
			result = append(result, [2]string{it.GetKey(), string(it.GetValue())})
		}
		return nil
	})

	w.Assert.NoError(err)

	w.Put("result", result)
})

var _ = steps.Then("I should retrieve entries in order", func(w *world.World) {
	w.Require.Equal([][2]string{
		{"00", ""},
		{"01", ""},
		{"02", ""},
		{"03", ""},
		{"04", ""},
		{"05", "0"},
		{"06", "1"},
		{"07", "2"},
		{"08", "3"},
		{"09", "4"},
	},
		w.Attributes["result"])
})
