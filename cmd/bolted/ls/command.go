package ls

import (
	"fmt"
	"time"

	"github.com/draganm/bolted"
	"github.com/draganm/bolted/dbpath"
	"github.com/draganm/bolted/embedded"
	"github.com/urfave/cli/v2"
	"go.etcd.io/bbolt"
)

var Command = &cli.Command{
	Name:      "ls",
	Aliases:   []string{"list"},
	Usage:     "list one bucket in the database",
	ArgsUsage: "<database file> [db path]",
	Flags: []cli.Flag{
		&cli.DurationFlag{
			Usage:   "timeout for opening the database",
			Name:    "open-timeout",
			Value:   500 * time.Millisecond,
			EnvVars: []string{"OPEN_TIMEOUT"},
		},
	},
	Action: func(c *cli.Context) error {
		if c.NArg() < 1 {
			return fmt.Errorf("db file and path must be provided")
		}

		sourceFile := c.Args().Get(0)

		p := c.Args().Get(1)
		if p == "" {
			p = "/"
		}
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

		return db.Read(func(tx bolted.ReadTx) error {
			for it := tx.Iterator(dbp); !it.IsDone(); it.Next() {

				suffix := ""
				if it.GetValue() == nil {
					suffix = "/"
				}
				fmt.Println(it.GetKey() + suffix)
			}
			return nil
		})

	},
}
