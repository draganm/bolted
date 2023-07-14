package bolted

import (
	"os"

	"github.com/draganm/bolted/dbt"
	"github.com/draganm/bolted/local"
)

type Options local.Options

func OpenLocalDB(path string, mode os.FileMode, options Options) (dbt.Database, error) {
	return local.Open(path, mode, local.Options(options))
}
