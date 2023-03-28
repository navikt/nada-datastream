package google

import (
	"context"
)

func (g *Google) EnableAPIs(ctx context.Context) error {
	// annet sted
	apis := []string{
		"bigquery.googleapis.com",
		"compute.googleapis.com",
		"servicenetworking.googleapis.com",
	}

	for _, a := range apis {
		g.log.Infof("Enabling API %v...", a)
		err := g.performRequest(ctx, []string{
			"services",
			"enable",
			a,
		}, nil)
		if err != nil {
			g.log.WithError(err).Errorf("enabling api %v", a)
			return err
		}
		g.log.Infof("Done")
	}

	return nil
}
