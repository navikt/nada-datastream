package google

import (
	"context"
	"fmt"
	"strings"

	"gopkg.in/yaml.v2"
)

const (
	proxyVMName            = "datastream"
	cloudsqlContainerImage = "gcr.io/cloud-sql-connectors/cloud-sql-proxy:2.1.1-alpine"
)

type sqlInstance struct {
	Name        string              `json:"name"`
	IpAddresses []map[string]string `json:"ipAddresses"`
	Settings    struct {
		DatabaseFlags []map[string]string `json:"databaseFlags"`
	} `json:"settings"`
}

func (g *Google) PatchCloudSQLInstance(ctx context.Context) error {
	patched, err := g.checkCloudSQLInstanceStatus(ctx)
	if err != nil {
		return err
	}
	if patched {
		return nil
	}

	g.log.Infof("Patching SQL instance...")
	err = g.performRequest(ctx, []string{
		"sql",
		"instances",
		"patch",
		g.Instance,
		fmt.Sprintf("--network=%v", vpcName),
	}, nil)
	if err != nil {
		g.log.WithError(err).Errorf("patching sql instance %v", g.Instance)
		return err
	}

	return nil
}

func (g *Google) CreateCloudSQLProxy(ctx context.Context) error {
	if err := g.createSAIfNotExists(ctx); err != nil {
		return err
	}

	if err := g.grantSARoles(ctx); err != nil {
		return err
	}

	if err := g.createOrUpdateCloudSQLProxy(ctx); err != nil {
		return err
	}

	return nil
}

func (g *Google) checkCloudSQLInstanceStatus(ctx context.Context) (bool, error) {
	instances := []*sqlInstance{}

	err := g.performRequest(ctx, []string{
		"sql",
		"instances",
		"list",
	}, &instances)
	if err != nil {
		return false, err
	}

	for _, i := range instances {
		if i.Name == g.Instance {
			if g.hasPrivateIP(i.IpAddresses) {
				return true, nil
			} else {
				return false, nil
			}
		}
	}

	return false, fmt.Errorf("no cloudsql instance with name %v in project %v", g.Instance, g.Project)
}

func (g *Google) hasPrivateIP(ipAddresses []map[string]string) bool {
	for _, ip := range ipAddresses {
		if ip["type"] == "PRIVATE" {
			return true
		}
	}

	return false
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
	iamPolicies := []*iamPolicy{} //[]map[string]any{}

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

func (g *Google) createOrUpdateCloudSQLProxy(ctx context.Context) error {
	exists, err := g.cloudSQLProxyExists(ctx)
	if err != nil {
		return err
	}
	if exists {
		if err := g.updateCloudSQLProxy(ctx); err != nil {
			return err
		}
		return nil
	}

	g.log.Infof("Creating CloudSQL proxy VM...")
	if g.CloudSQLPrivateIP {
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
			fmt.Sprintf("--network-interface=network=%v,subnet=%v,no-address", vpcName, vpcName),
			fmt.Sprintf("--container-image=%v", cloudsqlContainerImage),
			fmt.Sprintf(`--container-arg=%v:%v:%v`, g.Project, g.Region, g.Instance),
			`--container-arg=--address=0.0.0.0`,
			`--container-arg=--private-ip`,
		}, nil)
	} else {
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
			fmt.Sprintf(`--container-arg=%v:%v:%v`, g.Project, g.Region, g.Instance),
			`--container-arg=--address=0.0.0.0`,
		}, nil)
	}
	if err != nil {
		return err
	}

	return nil
}

func (g *Google) cloudSQLProxyExists(ctx context.Context) (bool, error) {
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
		if i.Name == "datastream" {
			return true, nil
		}
	}

	return false, nil
}

func (g *Google) getProxyIP(ctx context.Context) (string, error) {
	type DBInstance struct {
		NetworkInterfaces []struct {
			Network   string `json:"network"`
			NetworkIP string `json:"networkIP"`
		} `json:"networkInterfaces"`
	}
	instance := DBInstance{}

	err := g.performRequest(ctx, []string{
		"compute",
		"instances",
		"describe",
		"datastream",
		"--zone=europe-north1-b",
	}, &instance)
	if err != nil {
		return "", err
	}

	if len(instance.NetworkInterfaces) == 0 {
		return "", fmt.Errorf("compute instance datastream does not exist in project %v", g.Project)
	}

	for _, n := range instance.NetworkInterfaces {
		nParts := strings.Split(n.Network, "/")
		if nParts[len(nParts)-1] == vpcName {
			return n.NetworkIP, nil
		}
	}

	return "", fmt.Errorf("compute instance datastream does not have expected network interface %v", vpcName)
}

func (g *Google) updateCloudSQLProxy(ctx context.Context) error {
	updatedSQLInstanceList, err := g.proxyVMNeedsUpdate(ctx)
	if err != nil {
		return err
	}
	if updatedSQLInstanceList == "" {
		return nil
	}

	g.log.Infof("Updating CloudSQL proxy VM...")
	if g.CloudSQLPrivateIP {
		err = g.performRequest(ctx, []string{
			"compute",
			"instances",
			"update-container",
			proxyVMName,
			fmt.Sprintf("--container-arg=%v", updatedSQLInstanceList),
			`--container-arg=--address=0.0.0.0`,
		}, nil)
	} else {
		err = g.performRequest(ctx, []string{
			"compute",
			"instances",
			"update-container",
			proxyVMName,
			fmt.Sprintf("--container-arg=%v", updatedSQLInstanceList),
			`--container-arg=--address=0.0.0.0`,
			`--container-arg=--private-ip`,
		}, nil)
	}
	if err != nil {
		return err
	}

	return nil
}

func (g *Google) proxyVMNeedsUpdate(ctx context.Context) (string, error) {
	type containerSpec struct {
		Spec struct {
			Containers []struct {
				Args []string `json:"args"`
			} `json:"containers"`
		} `json:"spec"`
	}

	type proxyVMInfo struct {
		Metadata struct {
			Items []map[string]string `json:"items"`
		} `json:"metadata"`
	}
	proxyVM := &proxyVMInfo{}

	err := g.performRequest(ctx, []string{
		"compute",
		"instances",
		"describe",
		proxyVMName,
		"--zone=europe-north1-b",
	}, &proxyVM)
	if err != nil {
		return "", err
	}

	for _, i := range proxyVM.Metadata.Items {
		if i["key"] == "gce-container-declaration" {
			spec := containerSpec{}
			if err := yaml.Unmarshal([]byte(i["value"]), &spec); err != nil {
				return "", err
			}
			if len(spec.Spec.Containers) != 1 {
				return "", fmt.Errorf("cloudsql proxy container declaration should contain one (and only one) container, got %v", len(spec.Spec.Containers))
			}

			for _, a := range spec.Spec.Containers[0].Args {
				if strings.Contains(a, g.Project+":") {
					if !strings.Contains(a, fmt.Sprintf("%v:%v:%v", g.Project, g.Region, g.Instance)) {
						return a + fmt.Sprintf(",%v:%v:%v", g.Project, g.Region, g.Instance), nil
					}
				}
			}
		}
	}

	return "", nil
}