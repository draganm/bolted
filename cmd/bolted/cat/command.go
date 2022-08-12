package cat

import (
	"fmt"
	"os"
	"time"

	"github.com/draganm/bolted"
	"github.com/draganm/bolted/dbpath"
	"github.com/draganm/bolted/embedded"
	"github.com/urfave/cli/v2"
	"go.etcd.io/bbolt"
)

var Command = &cli.Command{
	Name:      "cat",
	Aliases:   []string{"list"},
	Usage:     "print value from the database path",
	ArgsUsage: "<database file> <db path>",
	Flags: []cli.Flag{
		&cli.DurationFlag{
			Usage:   "timeout for opening the database",
			Name:    "open-timeout",
			Value:   500 * time.Millisecond,
			EnvVars: []string{"OPEN_TIMEOUT"},
		},
	},
	Action: func(c *cli.Context) error {
		if c.NArg() != 2 {
			return fmt.Errorf("db file and path must be provided")
		}

		sourceFile := c.Args().Get(0)
		p := c.Args().Get(1)
		dbp, err := dbpath.Parse(p)
		if err != nil {
			return fmt.Errorf("while parsing path %s: %w", p, err)
		}
		db, err := embedded.Open(sourceFile, 0700, embedded.Options{
			Options: bbolt.Options{
				Timeout:  c.Duration("open-timeout"),
				ReadOnly: true,
			},
		})

		if err != nil {
			return fmt.Errorf("while opening database: %w", err)
		}

		return bolted.SugaredRead(db, func(tx bolted.SugaredReadTx) error {
			val := tx.Get(dbp)
			_, err := os.Stdout.Write(val)
			if err != nil {
				return err
			}
			return nil
		})

	},
}