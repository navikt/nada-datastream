package datastream

import (
	"context"

	"github.com/navikt/nada-datastream/cmd"
	"github.com/navikt/nada-datastream/pkg/google"
	"github.com/navikt/nada-datastream/pkg/k8s"
	"github.com/sirupsen/logrus"
)

func GetDBConfig(ctx context.Context, appName, dbUser, context, namespace string, log *logrus.Logger) (*cmd.DBConfig, error) {
	log.Info("Retrieving datastream configurations...")
	k8sClient, err := k8s.New(context, namespace)
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
	return google.New(log.WithFields(logrus.Fields{}), cfg).CreateResources(ctx)
}

func Delete(ctx context.Context, cfg *cmd.Config, log *logrus.Logger) error {
	return google.New(log.WithFields(logrus.Fields{}), cfg).DeleteResources(ctx)
}
