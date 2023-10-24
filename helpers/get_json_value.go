package helpers

import (
	"encoding/json"
	"fmt"

	"github.com/draganm/bolted"
	"github.com/draganm/bolted/dbpath"
)

func GetJSONValue[T any](tx bolted.ReadTx, pth dbpath.Path) (*T, error) {
	v := new(T)

	err := json.Unmarshal(tx.Get(pth), v)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal %s: %w", pth, err)
	}

	return v, nil
}
