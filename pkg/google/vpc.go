package google

import (
	"context"
	"encoding/json"
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

	g.log.Info("Creating VPC...")
	_, err = g.performRequest(ctx, []string{
		"compute",
		"networks",
		"create",
		vpcName,
	})
	if err != nil {
		g.log.WithError(err).Errorf("creating vpc %v", vpcName)
		return err
	}
	g.log.Info("Done")

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
	vpcs, err := g.performRequest(ctx, []string{
		"compute",
		"networks",
		"list",
	})
	if err != nil {
		g.log.WithError(err).Errorf("listing VPCs in project %v", g.project)
		return false, err
	}

	for _, existing := range vpcs {
		if existing["name"] == vpcName {
			return true, nil
		}
	}

	return false, nil
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
	_, err = g.performRequest(ctx, []string{
		"compute",
		"addresses",
		"create",
		addressRangeName,
		"--global",
		"--purpose=VPC_PEERING",
		"--prefix-length=24",
		fmt.Sprintf("--network=%v", vpcName),
	})
	if err != nil {
		g.log.WithError(err).Errorf("creating private address range for cloudsql instance", g.instance)
		return err
	}
	g.log.Info("Done")

	return nil
}

func (g *Google) addressRangeExists(ctx context.Context) (bool, error) {
	addressRanges, err := g.performRequest(ctx, []string{
		"compute",
		"addresses",
		"list",
	})
	if err != nil {
		g.log.WithError(err).Errorf("listing addresses in project %v", g.project)
		return false, err
	}

	for _, ar := range addressRanges {
		if ar["name"] == addressRangeName {
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
	_, err = g.performRequest(ctx, []string{
		"services",
		"vpc-peerings",
		"update",
		"--service=servicenetworking.googleapis.com",
		fmt.Sprintf("--network=%v", vpcName),
		fmt.Sprintf("--ranges=%v", addressRangeName),
		"--force",
	})
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

	peerings, err := g.performRequest(ctx, []string{
		"services",
		"vpc-peerings",
		"list",
		fmt.Sprintf("--network=%v", vpcName),
	})
	if err != nil {
		g.log.WithError(err).Errorf("listing peerings for VPC %v", vpcName)
		return false, err
	}

	for _, p := range peerings {
		pBytes, err := json.Marshal(p)
		if err != nil {
			return false, err
		}
		pRes := peeringRes{}
		if err := json.Unmarshal(pBytes, &pRes); err != nil {
			return false, err
		}

		if contains(pRes.ReservedPeeringRanges, addressRangeName) {
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
