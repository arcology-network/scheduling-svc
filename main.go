package main

import (
	"os"

	"github.com/HPISTechnologies/3rd-party/tm/cli"
	"github.com/HPISTechnologies/scheduling-svc/service"
)

func main() {

	st := service.StartCmd

	cmd := cli.PrepareMainCmd(st, "BC", os.ExpandEnv("$HOME/monacos/scheduling"))
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
