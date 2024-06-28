package root

import (
	"context"
	"fmt"
	"strings"

	dsCmd "github.com/navikt/nada-datastream/cmd"
	"github.com/navikt/nada-datastream/pkg/datastream"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var create = &cobra.Command{
	Use:   "create [app-name] [db-user] [flags]",
	Short: "Create a new datastream",
	Long:  `Create a new datastream`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) != 2 {
			return fmt.Errorf("Invalid number of arguments.")
		}

		ctx := context.Background()
		log := logrus.New()
		cfg := &dsCmd.Config{
			Publication:     "ds_publication",
			ReplicationSlot: "ds_replication",
		}

		appName := args[0]
		dbUser := args[1]

		namespace := viper.GetString(dsCmd.Namespace)
		context := viper.GetString(dsCmd.Context)

		included := viper.GetString(dsCmd.IncludeTables)
		if included != "" {
			cfg.IncludeTables = strings.Split(included, ",")
		}

		excluded := viper.GetString(dsCmd.ExcludeTables)
		if excluded != "" {
			cfg.ExcludeTables = strings.Split(excluded, ",")
		}

		publication := viper.GetString(dsCmd.PublicationName)
		if publication != "" {
			cfg.Publication = publication
		}
		replicationSlot := viper.GetString(dsCmd.ReplicationSlotName)
		if replicationSlot != "" {
			cfg.ReplicationSlot = replicationSlot
		}

		dataFreshness := viper.GetInt(dsCmd.DataFreshness)
		cfg.DataFreshness = dataFreshness

		dbCfg, err := datastream.GetDBConfig(ctx, appName, dbUser, context, namespace, log)
		if err != nil {
			return err
		}
		cfg.DBConfig = dbCfg

		if err := datastream.Create(ctx, cfg, log); err != nil {
			return err
		}

		return nil
	},
}

func init() {
	create.PersistentFlags().String(dsCmd.IncludeTables, "", "comma separated list of tables in postgres db that should be included in the datastream")
	viper.BindPFlag(dsCmd.IncludeTables, create.PersistentFlags().Lookup(dsCmd.IncludeTables))
	create.PersistentFlags().String(dsCmd.ExcludeTables, "", "comma separated list of tables in postgres db that should be excluded from the datastream")
	viper.BindPFlag(dsCmd.ExcludeTables, create.PersistentFlags().Lookup(dsCmd.ExcludeTables))
	create.PersistentFlags().String(dsCmd.ReplicationSlotName, "", "name the of replication slot in database (defaults to 'ds_replication')")
	viper.BindPFlag(dsCmd.ReplicationSlotName, create.PersistentFlags().Lookup(dsCmd.ReplicationSlotName))
	create.PersistentFlags().String(dsCmd.PublicationName, "", "name the of publication in database (defaults to 'ds_publication')")
	viper.BindPFlag(dsCmd.PublicationName, create.PersistentFlags().Lookup(dsCmd.PublicationName))
	create.PersistentFlags().Int(dsCmd.DataFreshness, 900, "data freshness in seconds (how often data is fetched from database and stored in bigquery)")
	viper.BindPFlag(dsCmd.DataFreshness, create.PersistentFlags().Lookup(dsCmd.DataFreshness))

	rootCmd.AddCommand(create)
}
