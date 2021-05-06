package dbpath_test

import (
	"testing"

	"github.com/draganm/bolted/dbpath"
	"github.com/stretchr/testify/require"
)

func TestCanonical(t *testing.T) {
	cases := []struct {
		name   string
		input  string
		output string
		error  string
	}{
		{
			name:   "empty path",
			input:  "",
			output: "",
		},
		{
			name:   "empty path /",
			input:  "/",
			output: "",
		},
		{
			name:   "empty path //",
			input:  "//",
			output: "",
		},
		{
			name:   "single value",
			input:  "abc",
			output: "abc",
		},
		{
			name:   "two values",
			input:  "abc/def",
			output: "abc/def",
		},
		{
			name:   "two values with / prefix",
			input:  "/abc/def",
			output: "abc/def",
		},
		{
			name:   "two values with / suffix",
			input:  "abc/def/",
			output: "abc/def",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			c := c
			output, err := dbpath.ToCanonical(c.input)
			require.Equal(t, c.output, output, "output does not match")
			if c.error == "" {
				require.NoError(t, err, "unexpected error returned")
			} else {
				require.Error(t, err, "expected an error")
				require.EqualError(t, err, c.error)
			}
			require.Equal(t, c.output, output, "output does not match")
		})
	}
}
