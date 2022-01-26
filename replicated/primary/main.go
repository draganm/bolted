package main

import (
	"fmt"
	"net/http"

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
				Name:    "wal-path",
				Value:   "logs/wal",
				EnvVars: []string{"WAL_PATH"},
			},
		},
		Action: func(c *cli.Context) error {

			ps, err := server.New(c.String("wal-path"))
			if err != nil {
				return fmt.Errorf("whole creating primary server: %w", err)
			}

			return http.ListenAndServe(c.String("addr"), ps)
		},
	}
	app.RunAndExitOnError()
}
