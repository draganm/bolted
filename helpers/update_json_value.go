package helpers

import (
	"encoding/json"
	"fmt"

	"github.com/draganm/bolted"
	"github.com/draganm/bolted/dbpath"
)

func UpdateJSONValue[T any](tx bolted.WriteTx, pth dbpath.Path, update func(v *T) error) error {
	v := new(T)

	err := json.Unmarshal(tx.Get(pth), v)
	if err != nil {
		return fmt.Errorf("could not unmarshal %s: %w", pth, err)
	}

	err = update(v)
	if err != nil {
		return fmt.Errorf("could not update %s: %w", pth, err)
	}

	d, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("could not marshal %s: %w", pth, err)
	}

	tx.Put(pth, d)

	return nil

}
