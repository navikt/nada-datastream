package google

import (
	"context"
	"fmt"
	"strings"
)

const (
	proxyVMNamePrefix      = "datastream-"
	cloudsqlContainerImage = "gcr.io/cloud-sql-connectors/cloud-sql-proxy:2.1.1-alpine"
	machineType            = "g1-small"
	serviceAccountName     = "datastream"
)

type sqlInstance struct {
	Name        string              `json:"name"`
	IpAddresses []map[string]string `json:"ipAddresses"`
	Settings    struct {
		DatabaseFlags []map[string]string `json:"databaseFlags"`
	} `json:"settings"`
}

func (g *Google) SAID(sa string) string {
	return fmt.Sprintf("%v@%v.iam.gserviceaccount.com", sa, g.Project)
}

func (g Google) saExists(ctx context.Context, serviceAccount string) (bool, error) {
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
		if sa.Email == g.SAID(serviceAccount) {
			return true, nil
		}
	}

	return false, nil
}

func (g Google) createSAAndGrantRoles(ctx context.Context, serviceAccount string) error {
	err := g.createSA(ctx, serviceAccount)
	if err != nil {
		return err
	}

	return g.grantSARoles(ctx, serviceAccount)
}

func (g Google) createSA(ctx context.Context, serviceAccount string) error {
	err := g.performRequest(ctx, []string{
		"iam",
		"service-accounts",
		"create",
		serviceAccount,
		`--description="Datastream service account"`,
		"--display-name=datastream",
	}, map[string]string{})
	if err != nil {
		return err
	}

	return nil
}

func (g *Google) grantSARoles(ctx context.Context, serviceAccount string) error {
	exists, err := g.rolebindingsExist(ctx, "roles/cloudsql.client")
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
		fmt.Sprintf("--member=serviceAccount:%v", g.SAID(serviceAccount)),
		"--role=roles/cloudsql.client",
		"--condition=None",
	}, nil)
	if err != nil {
		return err
	}

	return nil
}

func (g *Google) rolebindingsExist(ctx context.Context, role string) (bool, error) {
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
		if b.Bindings.Role == role {
			return true, nil
		}
	}

	return false, nil
}

func (g Google) createCloudSQLProxy(ctx context.Context, proxyName string) error {
	g.log.Infof("Creating CloudSQL proxy VM...")
	said := g.SAID(generateNameFunc[SERVICE_ACCOUNT](&g))
	vpcid := generateNameFunc[VPC](&g)
	err := g.performRequest(ctx, []string{
		"compute",
		"instances",
		"create-with-container",
		proxyName,
		fmt.Sprintf("--machine-type=%v", machineType),
		"--zone=europe-north1-b",
		fmt.Sprintf("--service-account=%v", said),
		"--create-disk=image-project=debian-cloud,image-family=debian-11",
		"--scopes=cloud-platform",
		fmt.Sprintf("--network-interface=network=%v,subnet=%v", vpcid, vpcid),
		fmt.Sprintf("--container-image=%v", cloudsqlContainerImage),
		fmt.Sprintf(`--container-arg=%v:%v:%v?port=5432`, g.Project, g.Region, g.Instance),
		`--container-arg=--address=0.0.0.0`,
	}, nil)
	if err != nil {
		return err
	}

	return nil
}

func (g Google) cloudSQLProxyExists(ctx context.Context, proxyVMName string) (bool, error) {
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

func (g Google) deleteCloudSQLProxy(ctx context.Context, proxyVMName string) error {
	g.log.Infof("Deleting CloudSQL proxy VM...")
	return g.performRequest(ctx, []string{
		"compute",
		"instances",
		"delete",
		proxyVMName,
		"--zone=europe-north1-b",
		"--quiet",
	}, nil)
}

func (g *Google) removeSARoles(ctx context.Context) error {
	g.log.Infof("Remove CloudSQL Client role with VM service account...")
	said := g.SAID(generateNameFunc[SERVICE_ACCOUNT](g))
	return g.performRequest(ctx, []string{
		"projects",
		"remove-iam-policy-binding",
		g.Project,
		fmt.Sprintf("--member=serviceAccount:%v", said),
		"--role=roles/cloudsql.client",
		"--condition=None",
	}, nil)
}

func (g Google) deleteSA(ctx context.Context, serviceAccount string) error {
	g.log.Infof("Deleting IAM service account for VM...")
	return g.performRequest(ctx, []string{
		"iam",
		"service-accounts",
		"delete",
		g.SAID(serviceAccount),
	}, nil)
}
