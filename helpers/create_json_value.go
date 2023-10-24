// Package helpers provides utility functions to work with bolted DB.
package helpers

import (
	"encoding/json"
	"fmt"

	"github.com/draganm/bolted"
	"github.com/draganm/bolted/dbpath"
)

// CreateJSONValue is a utility function that creates, initializes, and stores a JSON representation of a value
// at a specified path within a write transaction. The value is of type T, instantiated within the function, and
// its initialization is handled by a callback function provided by the caller.
//
// Parameters:
//   - tx: The write transaction within which the operation is performed. It must be active and not nil.
//   - pth: The dbpath.Path instance where the JSON value is to be stored within the transaction. It must be a valid path.
//   - fn: A callback function provided by the caller to initialize the value. It accepts an instance of the type *T
//
// Returns:
// An error if there was an issue with creating the value or marshalling it to JSON. Otherwise, it returns nil.
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
