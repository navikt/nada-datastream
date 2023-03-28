package google

import (
	"context"
	"fmt"
	"time"
)

const (
	privateConnectionName = "datastream-connection"
	datastreamSubnet      = "10.2.0.0/29" // arbitrary
	firewallRuleName      = "allow-datastream-cloudsql-proxy"
)

func (g *Google) CreateDatastreamPrivateConnection(ctx context.Context) error {
	if err := g.createPrivateConnection(ctx); err != nil {
		return err
	}

	if err := g.createDatastreamFirewallRule(ctx); err != nil {
		return err
	}

	if err := g.waitForPrivateConnectionUp(ctx); err != nil {
		return err
	}

	return nil
}

func (g *Google) CreatePostgresProfile(ctx context.Context) error {
	if err := g.createPostgresProfile(ctx); err != nil {
		return err
	}

	return nil
}

func (g *Google) createPrivateConnection(ctx context.Context) error {
	exists, err := g.privateConnectionExists(ctx)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	err = g.performRequest(ctx, []string{
		"datastream",
		"private-connections",
		"create",
		privateConnectionName,
		fmt.Sprintf("--display-name=%v", privateConnectionName),
		fmt.Sprintf("--vpc=%v", vpcName),
		fmt.Sprintf("--subnet=%v", datastreamSubnet),
		fmt.Sprintf("--location=%v", g.region),
	}, nil)
	if err != nil {
		return err
	}

	return nil
}

func (g *Google) privateConnectionExists(ctx context.Context) (bool, error) {
	type privateConn struct {
		Name string `json:"name"`
	}
	privateCons := []*privateConn{}

	err := g.performRequest(ctx, []string{
		"datastream",
		"private-connections",
		"list",
		fmt.Sprintf("--location=%v", g.region),
	}, &privateCons)
	if err != nil {
		return false, err
	}

	for _, c := range privateCons {
		if c.Name == fmt.Sprintf("projects/%v/locations/%v/privateConnections/%v", g.project, g.region, privateConnectionName) {
			return true, nil
		}
	}

	return false, nil
}

func (g *Google) createDatastreamFirewallRule(ctx context.Context) error {
	exists, err := g.datastreamFirewallRuleExists(ctx)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	err = g.performRequest(ctx, []string{
		"compute",
		"firewall-rules",
		"create",
		firewallRuleName,
		fmt.Sprintf("--source-ranges=%v", datastreamSubnet),
		fmt.Sprintf("--network=%v", vpcName),
		"--allow=tcp:5432",
		"--direction=INGRESS",
	}, nil)
	if err != nil {
		return err
	}

	return nil
}

func (g *Google) waitForPrivateConnectionUp(ctx context.Context) error {
	type privateConnection struct {
		Name  string `json:"name"`
		State string `json:"state"`
	}
	privCons := []*privateConnection{}

	for {
		err := g.performRequest(ctx, []string{
			"datastream",
			"private-connections",
			"list",
			fmt.Sprintf("--location=%v", g.region),
			fmt.Sprintf("--filter=name=projects/%v/locations/%v/privateConnections/%v", g.project, g.region, privateConnectionName),
		}, &privCons)
		if err != nil {
			return err
		}
		if len(privCons) != 1 {
			return fmt.Errorf("should be one (and only one) private connection named %v, but got %v", privateConnectionName, len(privCons))
		}

		switch privCons[0].State {
		case "CREATING":
			g.log.Info("Waiting for datastream private connection up")
			time.Sleep(30 * time.Second)
			continue
		case "CREATED":
			return nil
		default:
			return fmt.Errorf("datastream private connection creation invalid state: %v (should be either CREATING or CREATED)", privCons[0].State)
		}
	}
}

func (g *Google) datastreamFirewallRuleExists(ctx context.Context) (bool, error) {
	type firewallRule struct {
		Name string `json:"name"`
	}
	firewallRules := []*firewallRule{}

	err := g.performRequest(ctx, []string{
		"compute",
		"firewall-rules",
		"list",
	}, &firewallRules)
	if err != nil {
		return false, err
	}

	for _, r := range firewallRules {
		if r.Name == firewallRuleName {
			return true, nil
		}
	}

	return false, nil
}

func (g *Google) CreateDatastreamProfiles(ctx context.Context) error {
	if err := g.createPostgresProfile(ctx); err != nil {
		return err
	}

	if err := g.createBigqueryProfile(ctx); err != nil {
		return err
	}

	return nil
}

func (g *Google) createPostgresProfile(ctx context.Context) error {
	profileName := fmt.Sprintf("postgres-%v", g.db)
	exists, err := g.profileExists(ctx, profileName)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	host, err := g.getProxyIP(ctx)
	if err != nil {
		return err
	}

	err = g.performRequest(ctx, []string{
		"datastream",
		"connection-profiles",
		"create",
		profileName,
		fmt.Sprintf("--display-name=postgres-%v", g.db),
		"--type=postgresql",
		fmt.Sprintf("--location=%v", g.region),
		fmt.Sprintf("--private-connection=%v", privateConnectionName),
		fmt.Sprintf("--postgresql-database=%v", g.db),
		fmt.Sprintf("--postgresql-hostname=%v", host),
		fmt.Sprintf("--postgresql-username=%v", g.user),
		fmt.Sprintf("--postgresql-password=%v", g.password),
		fmt.Sprintf("--postgresql-port=%v", g.port),
	}, nil)
	if err != nil {
		return err
	}

	return nil
}

func (g *Google) createBigqueryProfile(ctx context.Context) error {
	profileName := fmt.Sprintf("bigquery-%v", g.db)
	exists, err := g.profileExists(ctx, profileName)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	err = g.performRequest(ctx, []string{
		"datastream",
		"connection-profiles",
		"create",
		profileName,
		fmt.Sprintf("--display-name=bigquery-%v", g.db),
		"--type=bigquery",
		fmt.Sprintf("--location=%v", g.region),
	}, nil)
	if err != nil {
		return err
	}

	return nil
}

func (g *Google) profileExists(ctx context.Context, profileName string) (bool, error) {
	type profile struct {
		Name string `json:"name"`
	}
	profiles := []*profile{}

	err := g.performRequest(ctx, []string{
		"datastream",
		"connection-profiles",
		"list",
		fmt.Sprintf("--location=%v", g.region),
	}, &profiles)
	if err != nil {
		return false, err
	}

	for _, p := range profiles {
		if p.Name == fmt.Sprintf("projects/%v/locations/%v/connectionProfiles/%v", g.project, g.region, profileName) {
			return true, nil
		}
	}

	return false, nil
}
