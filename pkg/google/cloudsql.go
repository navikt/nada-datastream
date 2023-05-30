package google

import (
	"context"
	"fmt"
	"strings"

	"github.com/navikt/nada-datastream/cmd"
)

const (
	proxyVMNamePrefix      = "datastream-"
	cloudsqlContainerImage = "gcr.io/cloud-sql-connectors/cloud-sql-proxy:2.1.1-alpine"
)

type sqlInstance struct {
	Name        string              `json:"name"`
	IpAddresses []map[string]string `json:"ipAddresses"`
	Settings    struct {
		DatabaseFlags []map[string]string `json:"databaseFlags"`
	} `json:"settings"`
}

func (g *Google) CreateCloudSQLProxy(ctx context.Context, cfg *cmd.Config) error {
	if err := g.createSAIfNotExists(ctx); err != nil {
		return err
	}

	if err := g.grantSARoles(ctx); err != nil {
		return err
	}

	if err := g.createCloudSQLProxy(ctx, cfg); err != nil {
		return err
	}

	return nil
}

func (g *Google) createSAIfNotExists(ctx context.Context) error {
	exists, err := g.saExists(ctx)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	g.log.Infof("Creating IAM service account for VM...")
	if err := g.createSA(ctx); err != nil {
		return err
	}

	return nil
}

func (g *Google) saExists(ctx context.Context) (bool, error) {
	type SA struct {
		Email string `json:"email"`
	}
	sas := []*SA{}

	err := g.performRequest(ctx, []string{
		"iam",
		"service-accounts",
		"list",
	}, &sas)
	if err != nil {
		return false, err
	}

	for _, sa := range sas {
		if sa.Email == fmt.Sprintf("datastream@%v.iam.gserviceaccount.com", g.Project) {
			return true, nil
		}
	}

	return false, nil
}

func (g *Google) createSA(ctx context.Context) error {
	err := g.performRequest(ctx, []string{
		"iam",
		"service-accounts",
		"create",
		"datastream",
		`--description="Datastream service account"`,
		"--display-name=datastream",
	}, map[string]string{})
	if err != nil {
		return err
	}

	return nil
}

func (g *Google) grantSARoles(ctx context.Context) error {
	exists, err := g.rolebindingsExist(ctx)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	g.log.Infof("Granting CloudSQL Client role to VM service account...")
	err = g.performRequest(ctx, []string{
		"projects",
		"add-iam-policy-binding",
		g.Project,
		fmt.Sprintf("--member=serviceAccount:datastream@%v.iam.gserviceaccount.com", g.Project),
		"--role=roles/cloudsql.client",
		"--condition=None",
	}, nil)
	if err != nil {
		return err
	}

	return nil
}

func (g *Google) rolebindingsExist(ctx context.Context) (bool, error) {
	type iamPolicy struct {
		Bindings struct {
			Role string `json:"role"`
		} `json:"bindings"`
	}
	iamPolicies := []*iamPolicy{}

	err := g.performRequest(ctx, []string{
		"projects",
		"get-iam-policy",
		g.Project,
		"--flatten=bindings[].members",
		fmt.Sprintf("--filter=bindings.members=serviceAccount:datastream@%v.iam.gserviceaccount.com", g.Project),
	}, &iamPolicies)
	if err != nil {
		return false, err
	}

	for _, b := range iamPolicies {
		if b.Bindings.Role == "roles/cloudsql.client" {
			return true, nil
		}
	}

	return false, nil
}

func (g *Google) createCloudSQLProxy(ctx context.Context, cfg *cmd.Config) error {
	proxyVMName := proxyVMNamePrefix + cfg.DB
	exists, err := g.cloudSQLProxyExists(ctx, proxyVMName)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	g.log.Infof("Creating CloudSQL proxy VM...")
	err = g.performRequest(ctx, []string{
		"compute",
		"instances",
		"create-with-container",
		proxyVMName,
		"--machine-type=f1-micro",
		"--zone=europe-north1-b",
		fmt.Sprintf("--service-account=datastream@%v.iam.gserviceaccount.com", g.Project),
		"--create-disk=image-project=debian-cloud,image-family=debian-11",
		"--scopes=cloud-platform",
		fmt.Sprintf("--network-interface=network=%v,subnet=%v", vpcName, vpcName),
		fmt.Sprintf("--container-image=%v", cloudsqlContainerImage),
		fmt.Sprintf(`--container-arg=%v:%v:%v?port=5432`, g.Project, g.Region, g.Instance),
		`--container-arg=--address=0.0.0.0`,
	}, nil)
	if err != nil {
		return err
	}

	return nil
}

func (g *Google) cloudSQLProxyExists(ctx context.Context, proxyVMName string) (bool, error) {
	type ComputeInstance struct {
		Name string `json:"name"`
	}
	instances := []*ComputeInstance{}

	err := g.performRequest(ctx, []string{
		"compute",
		"instances",
		"list",
	}, &instances)
	if err != nil {
		return false, err
	}

	for _, i := range instances {
		if i.Name == proxyVMName {
			return true, nil
		}
	}

	return false, nil
}

func (g *Google) getProxyIP(ctx context.Context, vmName string) (string, error) {
	type DBInstance struct {
		NetworkInterfaces []struct {
			Network   string `json:"network"`
			NetworkIP string `json:"networkIP"`
		} `json:"networkInterfaces"`
		Metadata struct {
			Items []map[string]string `json:"items"`
		} `json:"metadata"`
	}
	instance := DBInstance{}

	err := g.performRequest(ctx, []string{
		"compute",
		"instances",
		"describe",
		vmName,
		"--zone=europe-north1-b",
	}, &instance)
	if err != nil {
		return "", err
	}

	if len(instance.NetworkInterfaces) == 0 {
		return "", fmt.Errorf("datastream compute instance does not exist in project %v", g.Project)
	}

	for _, n := range instance.NetworkInterfaces {
		nParts := strings.Split(n.Network, "/")
		if nParts[len(nParts)-1] == vpcName {
			return n.NetworkIP, nil
		}
	}

	return "", fmt.Errorf("datastream compute instance does not have expected network interface %v", vpcName)
}

func (g *Google) DeleteCloudSQLProxy(ctx context.Context, cfg *cmd.Config) error {
	if err := g.removeSARoles(ctx); err != nil {
		return err
	}

	if err := g.deleteSA(ctx); err != nil {
		return err
	}

	if err := g.deleteCloudSQLProxy(ctx, cfg); err != nil {
		return err
	}

	return nil
}

func (g *Google) deleteCloudSQLProxy(ctx context.Context, cfg *cmd.Config) error {
	proxyVMName := proxyVMNamePrefix + cfg.DB

	g.log.Infof("Deleting CloudSQL proxy VM...")
	return g.performRequest(ctx, []string{
		"compute",
		"instances",
		"delete",
		proxyVMName,
		"--zone=europe-north1-b",
	}, nil)
}

func (g *Google) removeSARoles(ctx context.Context) error {
	g.log.Infof("Remove CloudSQL Client role with VM service account...")
	return g.performRequest(ctx, []string{
		"projects",
		"remove-iam-policy-binding",
		g.Project,
		fmt.Sprintf("--member=serviceAccount:datastream@%v.iam.gserviceaccount.com", g.Project),
		"--role=roles/cloudsql.client",
	}, nil)
}

func (g *Google) deleteSA(ctx context.Context) error {
	g.log.Infof("Deleting IAM service account for VM...")
	return g.performRequest(ctx, []string{
		"iam",
		"service-accounts",
		"delete",
		"datastream",
	}, nil)
}
