package helpers

import (
	"encoding/json"
	"fmt"

	"github.com/draganm/bolted"
	"github.com/draganm/bolted/dbpath"
)

func CreateJSONValue[T any](tx bolted.WriteTx, pth dbpath.Path, fn func(v *T) error) error {

	v := new(T)
	err := fn(v)
	if err != nil {
		return fmt.Errorf("could not create value for %s: %w", pth, err)
	}

	d, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("could not marshal value for %s: %w", pth, err)
	}

	tx.Put(pth, d)

	return nil
}
