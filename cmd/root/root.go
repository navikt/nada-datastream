package root

import (
	"context"

	dsCmd "github.com/navikt/nada-datastream/cmd"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var rootCmd = &cobra.Command{
	Use:   "nada-datastream",
	Short: "CLI for setting up datastream",
	Long:  `CLI for setting up datastream from cloudsql postgres to bigquery.`,
}

func Execute(ctx context.Context) error {
	rootCmd.PersistentFlags().StringP(dsCmd.Namespace, "n", "", "kubernetes namespace where the app is deployed (defaults to the one defined in kubeconfig)")
	viper.BindPFlag(dsCmd.Namespace, rootCmd.PersistentFlags().Lookup(dsCmd.Namespace))
	rootCmd.PersistentFlags().StringP(dsCmd.Context, "c", "", "kubernetes context where the app is deployed (defaults to the one defined in kubeconfig)")
	viper.BindPFlag(dsCmd.Context, rootCmd.PersistentFlags().Lookup(dsCmd.Context))

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		return err
	}

	return nil
}
