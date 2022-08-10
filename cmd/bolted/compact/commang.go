package compact

import (
	"fmt"
	"os"
	"time"

	"github.com/urfave/cli/v2"
	"go.etcd.io/bbolt"
)

var Command = &cli.Command{
	Name:      "compact",
	Usage:     "Create a compacted copy of the database",
	ArgsUsage: "<source db file> <destination db file>",
	Flags: []cli.Flag{
		&cli.IntFlag{
			Usage:   "Page size for the new database. Defaults to OS page size",
			Name:    "page-size",
			EnvVars: []string{"PAGE_SIZE"},
			Value:   os.Getpagesize(),
		},
		&cli.StringFlag{
			Usage:   "Type of the freelist for the new database: array | map",
			Name:    "freelist-type",
			EnvVars: []string{"FREELIST_TYPE"},
			Value:   "array",
		},
		&cli.DurationFlag{
			Usage:   "timeout for opening source database",
			Name:    "open-timeout",
			Value:   500 * time.Millisecond,
			EnvVars: []string{"OPEN_TIMEOUT"},
		},
	},

	Action: func(c *cli.Context) error {
		if c.NArg() != 2 {
			return fmt.Errorf("source and destination file names must be provided")
		}

		sourceFile := c.Args().Get(0)
		destinationFile := c.Args().Get(1)

		s, err := bbolt.Open(sourceFile, 0700, &bbolt.Options{Timeout: c.Duration("open-timeout"), ReadOnly: true})
		if err != nil {
			return fmt.Errorf("while opening source db: %w", err)
		}

		defer s.Close()

		flt, err := freelistType(c.String("freelist-type"))
		if err != nil {
			return err
		}

		d, err := bbolt.Open(destinationFile, 0700, &bbolt.Options{Timeout: c.Duration("open-timeout"), NoSync: true, PageSize: c.Int("page-size"), FreelistType: flt})
		if err != nil {
			return fmt.Errorf("while opening destination db: %w", err)
		}

		err = bbolt.Compact(d, s, 100000000)

		if err != nil {
			return fmt.Errorf("while compacting db: %w", err)
		}

		return nil

	},
}

func freelistType(t string) (bbolt.FreelistType, error) {
	switch t {
	case "map":
		return bbolt.FreelistMapType, nil
	case "array":
		return bbolt.FreelistArrayType, nil
	default:
		return "", fmt.Errorf("unknown freelist type: %s", t)
	}
}
