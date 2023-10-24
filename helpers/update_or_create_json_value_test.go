package helpers_test

import (
	"testing"

	"github.com/draganm/bolted"
	"github.com/draganm/bolted/helpers"
	"github.com/stretchr/testify/require"
)

func TestUpdateOrCreateJSONValue(t *testing.T) {
	require := require.New(t)
	db, cleanup := openEmptyDatabase(t, bolted.Options{})
	defer cleanup()

	err := db.Write(ctx, func(tx bolted.WriteTx) error {
		return helpers.UpdateOrCreateJSONValue(tx, fooPath, func(v *TestStruct) error {
			v.Bar = "X"
			return nil
		})
	})
	require.NoError(err)

	var read *TestStruct

	err = db.Read(ctx, func(tx bolted.ReadTx) error {
		read, err = helpers.GetJSONValue[TestStruct](tx, fooPath)
		return err
	})

	require.NoError(err)
	require.Equal(&TestStruct{Bar: "X"}, read)
}
