package helpers_test

import (
	"testing"

	"github.com/draganm/bolted"
	"github.com/draganm/bolted/dbpath"
	"github.com/draganm/bolted/helpers"
	"github.com/stretchr/testify/require"
)

type TestStruct struct {
	Foo string `json:"foo"`
	Bar string `json:"bar"`
}

var fooPath = dbpath.ToPath("foo")

func TestCreateJSONValue(t *testing.T) {
	require := require.New(t)
	db, cleanup := openEmptyDatabase(t, bolted.Options{})
	defer cleanup()

	err := db.Write(ctx, func(tx bolted.WriteTx) error {
		return helpers.CreateJSONValue(
			tx,
			fooPath,
			func(v *TestStruct) error {
				v.Foo = "baz"
				return nil
			},
		)
	})

	require.NoError(err)

	err = db.Write(ctx, func(tx bolted.WriteTx) error {
		return helpers.UpdateJSONValue(tx, fooPath, func(v *TestStruct) error {
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
	require.Equal(&TestStruct{Foo: "baz", Bar: "X"}, read)
}
