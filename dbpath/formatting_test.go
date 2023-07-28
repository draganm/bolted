package dbpath_test

import (
	"fmt"
	"testing"

	"github.com/draganm/bolted/dbpath"
	"github.com/stretchr/testify/require"
)

func TestSprintf(t *testing.T) {
	require := require.New(t)
	str := fmt.Sprintf("|%s|", dbpath.ToPath("abc", "def"))
	require.Equal("|abc/def|", str)

}
