package root

import (
	"context"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "nada-datastream",
	Short: "CLI for setting up datastream",
	Long:  `CLI for setting up datastream from cloudsql postgres to bigquery.`,
}

func Execute(ctx context.Context) error {
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		return err
	}

	return nil
}
