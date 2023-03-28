package google

import (
	"context"
	"fmt"
)

// global config annet sted?
const (
	vpcName          = "datastream-vpc"
	addressRangeName = "datastream-cloudsql"
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

	if err := g.enablePrivateGoogleAccessForVMSubnet(ctx); err != nil {
		return err
	}

	return nil
}

func (g *Google) PreparePrivateServiceConnectivity(ctx context.Context) error {
	if err := g.createAddressRange(ctx); err != nil {
		return err
	}

	if err := g.createPeering(ctx); err != nil {
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
		g.log.WithError(err).Errorf("listing VPCs in project %v", g.project)
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
	g.log.Info("Done")

	return nil
}

func (g *Google) enablePrivateGoogleAccessForVMSubnet(ctx context.Context) error {
	g.log.Info("Enabling Private Google Access...")
	err := g.performRequest(ctx, []string{
		"compute",
		"networks",
		"subnets",
		"update",
		vpcName,
		fmt.Sprintf("--region=%v", g.region),
		"--enable-private-ip-google-access",
	}, nil)
	if err != nil {
		return err
	}
	g.log.Info("Done")

	return nil
}

func (g *Google) createAddressRange(ctx context.Context) error {
	exists, err := g.addressRangeExists(ctx)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	g.log.Info("Creating private address range for cloudsql...")
	err = g.performRequest(ctx, []string{
		"compute",
		"addresses",
		"create",
		addressRangeName,
		"--global",
		"--purpose=VPC_PEERING",
		"--prefix-length=24",
		fmt.Sprintf("--network=%v", vpcName),
	}, nil)
	if err != nil {
		g.log.WithError(err).Errorf("creating private address range for cloudsql instance", g.instance)
		return err
	}
	g.log.Info("Done")

	return nil
}

func (g *Google) addressRangeExists(ctx context.Context) (bool, error) {
	type AddressRange struct {
		Name string `json:"name"`
	}
	addressRanges := []*AddressRange{}

	err := g.performRequest(ctx, []string{
		"compute",
		"addresses",
		"list",
	}, &addressRanges)
	if err != nil {
		g.log.WithError(err).Errorf("listing addresses in project %v", g.project)
		return false, err
	}

	for _, ar := range addressRanges {
		if ar.Name == addressRangeName {
			return true, nil
		}
	}

	return false, nil
}

func (g *Google) createPeering(ctx context.Context) error {
	exists, err := g.peeringExists(ctx)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	g.log.Info("Create peering between VPC and cloudsql private range...")
	err = g.performRequest(ctx, []string{
		"services",
		"vpc-peerings",
		"update",
		"--service=servicenetworking.googleapis.com",
		fmt.Sprintf("--network=%v", vpcName),
		fmt.Sprintf("--ranges=%v", addressRangeName),
		"--force",
	}, nil)
	if err != nil {
		g.log.WithError(err).Errorf("creating peering between VPC %v and cloudsql private range %v", vpcName, addressRangeName)
		return err
	}
	g.log.Info("Done")

	return nil
}

func (g *Google) peeringExists(ctx context.Context) (bool, error) {
	type peeringRes struct {
		ReservedPeeringRanges []string `json:"reservedPeeringRanges"`
	}
	peerings := []*peeringRes{}

	err := g.performRequest(ctx, []string{
		"services",
		"vpc-peerings",
		"list",
		fmt.Sprintf("--network=%v", vpcName),
	}, &peerings)
	if err != nil {
		g.log.WithError(err).Errorf("listing peerings for VPC %v", vpcName)
		return false, err
	}

	for _, p := range peerings {
		if contains(p.ReservedPeeringRanges, addressRangeName) {
			return true, nil
		}
	}

	return false, nil
}

func contains(vals []string, val string) bool {
	for _, v := range vals {
		if v == val {
			return true
		}
	}

	return false
}
