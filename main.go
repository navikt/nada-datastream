package main

import (
	"context"
	"fmt"
	"os"

	"github.com/navikt/nada-datastream/cmd/root"
)

func main() {
	ctx := context.Background()
	if err := root.Execute(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}
