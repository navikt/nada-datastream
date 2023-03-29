package main

import (
	"context"

	"github.com/navikt/nada-datastream/pkg/google"
	"github.com/sirupsen/logrus"
	flag "github.com/spf13/pflag"
)

func main() {
	cfg := google.Config{
		Region:            "europe-north1",
		Port:              "5432",
		CloudSQLPrivateIP: false,
	}
	ctx := context.Background()
	log := logrus.New()

	// Required input args
	flag.StringVar(&cfg.Project, "project", "", "GCP project")
	flag.StringVar(&cfg.Instance, "instance", "", "CloudSQL instance")
	flag.StringVar(&cfg.DB, "db", "", "CloudSQL db name")
	flag.StringVar(&cfg.User, "user", "", "Database user")
	flag.StringVar(&cfg.Password, "password", "", "Database password")

	flag.StringVar(&cfg.Region, "region", cfg.Region, "GCP region")
	flag.StringVar(&cfg.Port, "port", cfg.Port, "Database port")
	flag.BoolVar(&cfg.CloudSQLPrivateIP, "cloudsql-private-ip", cfg.CloudSQLPrivateIP, "Setup and use cloudsql private ip")
	flag.Parse()

	googleClient := google.New(log.WithField("subsystem", "google"), cfg)

	if err := googleClient.EnableAPIs(ctx); err != nil {
		log.Fatal(err)
	}

	if err := googleClient.CreateVPC(ctx); err != nil {
		log.Fatal(err)
	}

	if cfg.CloudSQLPrivateIP {
		if err := googleClient.PreparePrivateServiceConnectivity(ctx); err != nil {
			log.Fatal(err)
		}

		if err := googleClient.PatchCloudSQLInstance(ctx); err != nil {
			log.Fatal(err)
		}
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

	// if err := googleClient.CreateStream(ctx); err != nil {
	// 	log.Fatal(err)
	// }
}
