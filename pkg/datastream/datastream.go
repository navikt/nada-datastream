package datastream

import (
	"context"

	"github.com/navikt/nada-datastream/cmd"
	"github.com/navikt/nada-datastream/pkg/google"
	"github.com/navikt/nada-datastream/pkg/k8s"
	"github.com/sirupsen/logrus"
)

func GetDBConfig(ctx context.Context, appName, dbUser, namespace, context string, log *logrus.Logger) (*cmd.DBConfig, error) {
	k8sClient, err := k8s.New()
	if err != nil {
		log.Fatal(err)
	}

	cfg, err := k8sClient.DBConfig(ctx, appName, dbUser)
	if err != nil {
		return nil, err
	}

	return &cfg, nil
}

func Create(ctx context.Context, cfg *cmd.Config, log *logrus.Logger) error {
	googleClient := google.New(log.WithField("subsystem", "google"), cfg)

	if err := googleClient.EnableAPIs(ctx); err != nil {
		return err
	}

	if err := googleClient.CreateVPC(ctx); err != nil {
		return err
	}

	if err := googleClient.CreateCloudSQLProxy(ctx, cfg); err != nil {
		return err
	}

	if err := googleClient.CreateDatastreamPrivateConnection(ctx); err != nil {
		return err
	}

	if err := googleClient.CreateDatastreamProfiles(ctx); err != nil {
		return err
	}

	if err := googleClient.CreateStream(ctx); err != nil {
		return err
	}

	return nil
}

func Delete(ctx context.Context, cfg *cmd.Config, log *logrus.Logger) error {
	googleClient := google.New(log.WithField("subsystem", "google"), cfg)

	if err := googleClient.DeleteStream(ctx); err != nil {
		return err
	}

	if err := googleClient.DeleteDatastreamProfiles(ctx); err != nil {
		return err
	}

	if err := googleClient.DeleteDatastreamPrivateConnection(ctx); err != nil {
		return err
	}

	if err := googleClient.DeleteCloudSQLProxy(ctx, cfg); err != nil {
		return err
	}

	if err := googleClient.DeleteVPC(ctx); err != nil {
		return err
	}

	return nil
}
