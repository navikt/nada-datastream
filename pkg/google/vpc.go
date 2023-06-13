package google

import (
	"context"
)

// global config annet sted?
const (
	vpcName = "datastream-vpc"
)

func (g Google) vpcExists(ctx context.Context, vpc string) (bool, error) {
	type vpcType struct {
		Name string `json:"name"`
	}
	vpcs := []*vpcType{}

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
		if existing.Name == vpc {
			return true, nil
		}
	}

	return false, nil
}

func (g Google) createVPC(ctx context.Context, vpc string) error {
	g.log.Info("Creating VPC...")
	err := g.performRequest(ctx, []string{
		"compute",
		"networks",
		"create",
		vpc,
	}, nil)
	if err != nil {
		g.log.WithError(err).Errorf("creating vpc %v", vpc)
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

func (g Google) deleteVPC(ctx context.Context, vpc string) error {
	g.log.Info("Deleting VPC...")
	return g.performRequest(ctx, []string{
		"compute",
		"networks",
		"delete",
		vpc,
		"--quiet",
	}, nil)
}
