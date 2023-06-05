package google

import (
	"context"
)

// global config annet sted?
const (
	vpcName = "datastream-vpc"
)

func (g *Google) CreateVPC(ctx context.Context) error {
	exists, err := g.vpcExists(ctx)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	if err := g.createVPC(ctx); err != nil {
		return err
	}

	return nil
}

func (g *Google) vpcExists(ctx context.Context) (bool, error) {
	type vpc struct {
		Name string `json:"name"`
	}
	vpcs := []*vpc{}

	err := g.performRequest(ctx, []string{
		"compute",
		"networks",
		"list",
	}, &vpcs)
	if err != nil {
		g.log.WithError(err).Errorf("listing VPCs in project %v", g.Project)
		return false, err
	}

	for _, existing := range vpcs {
		if existing.Name == vpcName {
			return true, nil
		}
	}

	return false, nil
}

func (g *Google) createVPC(ctx context.Context) error {
	g.log.Info("Creating VPC...")
	err := g.performRequest(ctx, []string{
		"compute",
		"networks",
		"create",
		vpcName,
	}, nil)
	if err != nil {
		g.log.WithError(err).Errorf("creating vpc %v", vpcName)
		return err
	}

	return nil
}

func contains(vals []string, val string) bool {
	for _, v := range vals {
		if v == val {
			return true
		}
	}

	return false
}

func (g *Google) DeleteVPC(ctx context.Context) error {
	if streamExists, err := g.anyStreamExistis(ctx); streamExists || err != nil {
		return err
	}

	g.log.Info("Deleting VPC...")
	return g.performRequest(ctx, []string{
		"compute",
		"networks",
		"delete",
		vpcName,
	}, nil)
}
