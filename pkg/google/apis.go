package google

import (
	"context"
	"strings"
)

func (g *Google) EnableAPIs(ctx context.Context) error {
	// annet sted
	apis := []string{
		"bigquery.googleapis.com",
		"compute.googleapis.com",
		"datastream.googleapis.com",
		"run.googleapis.com",
		"servicenetworking.googleapis.com",
		"vpcaccess.googleapis.com",
	}

	enabled, err := g.listEnabledAPIs(ctx)
	if err != nil {
		return err
	}

	for _, a := range apis {
		if !contains(enabled, a) {
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
		}
	}

	return nil
}

func (g *Google) listEnabledAPIs(ctx context.Context) ([]string, error) {
	type api struct {
		Name string `json:"name"`
	}
	apis := []*api{}

	err := g.performRequest(ctx, []string{
		"services",
		"list",
		"--enabled",
	}, &apis)
	if err != nil {
		return nil, err
	}

	apiNames := []string{}
	for _, a := range apis {
		parts := strings.Split(a.Name, "/")
		apiNames = append(apiNames, parts[len(parts)-1])
	}

	return apiNames, nil
}
