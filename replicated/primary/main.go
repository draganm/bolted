package main

import "github.com/urfave/cli/v2"

func main() {
	app := &cli.App{
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "addr",
				Value:   ":9111",
				EnvVars: []string{"ADDR"},
			},
		},
		Action: func(c *cli.Context) error {

			return nil
		},
	}
	app.RunAndExitOnError()
}
