package dbpath_test

import (
	"testing"

	"github.com/draganm/bolted/dbpath"
	"github.com/stretchr/testify/require"
)

func TestAppend(t *testing.T) {
	cases := []struct {
		title          string
		path           string
		parts          []string
		expectedResult string
	}{
		{
			title:          "append empty parts to empty path",
			path:           "",
			parts:          nil,
			expectedResult: "",
		},
		{
			title:          "append one part to empty path",
			path:           "",
			parts:          []string{"test"},
			expectedResult: "test",
		},
		{
			title:          "append empty parts to not empty path",
			path:           "test",
			parts:          nil,
			expectedResult: "test",
		},
		{
			title:          "append one part to not empty path",
			path:           "test",
			parts:          []string{"foo", "bar"},
			expectedResult: "test/foo/bar",
		},
		{
			title:          "append one part to not empty path containing /",
			path:           "test/abc",
			parts:          []string{"foo", "bar"},
			expectedResult: "test/abc/foo/bar",
		},
	}

	for _, tc := range cases {
		t.Run(tc.title, func(t *testing.T) {
			require.Equal(t, tc.expectedResult, dbpath.Append(tc.path, tc.parts...))
		})
	}
}
