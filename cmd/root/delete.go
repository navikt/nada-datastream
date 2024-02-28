package root

import (
	"context"
	"fmt"

	dsCmd "github.com/navikt/nada-datastream/cmd"
	"github.com/navikt/nada-datastream/pkg/datastream"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var delete = &cobra.Command{
	Use:   "delete [app-name] [db-user]",
	Short: "Delete a datastream",
	Long:  `Delete a datastream`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) != 2 {
			return fmt.Errorf("Invalid number of arguments.")
		}

		ctx := context.Background()
		log := logrus.New()
		cfg := &dsCmd.Config{}

		appName := args[0]
		dbUser := args[1]

		namespace := viper.GetString(dsCmd.Namespace)
		context := viper.GetString(dsCmd.Context)
		dbCfg, err := datastream.GetDBConfig(ctx, appName, dbUser, context, namespace, log)
		if err != nil {
			return err
		}
		cfg.DBConfig = dbCfg

		if err := datastream.Delete(ctx, cfg, log); err != nil {
			return err
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(delete)
}
