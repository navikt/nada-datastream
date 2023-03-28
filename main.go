package main

import (
	"context"

	"github.com/navikt/nada-datastream/pkg/google"
	"github.com/sirupsen/logrus"
)

func main() {
	ctx := context.Background()
	log := logrus.New()

	googleClient := google.New(log.WithField("subsystem", "google"))

	if err := googleClient.EnableAPIs(ctx); err != nil {
		log.Fatal(err)
	}

	if err := googleClient.CreateVPC(ctx); err != nil {
		log.Fatal(err)
	}

	if err := googleClient.PreparePrivateServiceConnectivity(ctx); err != nil {
		log.Fatal(err)
	}

	if err := googleClient.PatchCloudSQLInstance(ctx); err != nil {
		log.Fatal(err)
	}

	if err := googleClient.CreateCloudSQLProxy(ctx); err != nil {
		log.Fatal(err)
	}

	if err := googleClient.CreateDatastreamPrivateConnection(ctx); err != nil {
		log.Fatal(err)
	}

	if err := googleClient.CreateDatastreamProfiles(ctx); err != nil {
		log.Fatal(err)
	}
}
