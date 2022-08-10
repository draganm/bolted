package main

import (
	"log"
	"os"

	"github.com/draganm/bolted/cmd/bolted/compact"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:        "bolted",
		Usage:       "Command line utility to inspect and manipulate bolted database files",
		HideVersion: true,
		Commands: []*cli.Command{
			compact.Command,
		},
	}
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
