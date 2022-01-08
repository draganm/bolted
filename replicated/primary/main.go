package main

import (
	"fmt"
	"net/http"

	"github.com/draganm/bolted/embedded"
	"github.com/draganm/bolted/replicated/primary/server"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "addr",
				Value:   ":9111",
				EnvVars: []string{"ADDR"},
			},
			&cli.StringFlag{
				Name:    "dbpath",
				Value:   "db",
				EnvVars: []string{"dbpath"},
			},
		},
		Action: func(c *cli.Context) error {

			db, err := embedded.Open(c.String("dbpath"), 0700)
			if err != nil {
				return err
			}

			ps, err := server.New(db)
			if err != nil {
				return fmt.Errorf("whole creating primary server: %w", err)
			}

			return http.ListenAndServe(c.String("addr"), ps)
		},
	}
	app.RunAndExitOnError()
}
