package google

import (
	"context"
	"fmt"
	"time"

	"github.com/navikt/nada-datastream/cmd"
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

func (g *Google) CreateStream(ctx context.Context) error {
	streamName := fmt.Sprintf("postgres-%v-bigquery", g.DB)
	exists, err := g.streamExists(ctx, streamName)
	if err != nil {
		return err
	}
	if exists {
		fmt.Println("debug: datastream exists")
		return nil
	}

	err = g.performRequest(ctx, []string{
		"datastream",
		"streams",
		"create",
		streamName,
		fmt.Sprintf("--display-name=%v", streamName),
		fmt.Sprintf("--location=%v", g.Region),
		fmt.Sprintf("--source=projects/%v/locations/%v/connectionProfiles/postgres-datastream", g.Project, g.Region),
		"--postgresql-source-config=pgconf.json",
		fmt.Sprintf("--destination=projects/%v/locations/%v/connectionProfiles/bigquery-datastream", g.Project, g.Region),
		"--bigquery-destination-config=bqconf.json",
		"--backfill-none",
	}, nil)
	if err != nil {
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

	g.log.Infof("Creating Datastream private connection...")
	err = g.performRequest(ctx, []string{
		"datastream",
		"private-connections",
		"create",
		privateConnectionName,
		fmt.Sprintf("--display-name=%v", privateConnectionName),
		fmt.Sprintf("--vpc=%v", vpcName),
		fmt.Sprintf("--subnet=%v", datastreamSubnet),
		fmt.Sprintf("--location=%v", g.Region),
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
		fmt.Sprintf("--location=%v", g.Region),
	}, &privateCons)
	if err != nil {
		return false, err
	}

	for _, c := range privateCons {
		if c.Name == fmt.Sprintf("projects/%v/locations/%v/privateConnections/%v", g.Project, g.Region, privateConnectionName) {
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

func (g *Google) updateDatastreamFirewallRule(ctx context.Context, cfg *cmd.Config) error {
	_, err := g.datastreamFirewallRuleExists(ctx)
	if err != nil {
		return err
	}

	err = g.performRequest(ctx, []string{
		"compute",
		"firewall-rules",
		"update",
		firewallRuleName,
		fmt.Sprintf("--allow=tcp:%v", "5432-"+cfg.Port),
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
			fmt.Sprintf("--location=%v", g.Region),
			fmt.Sprintf("--filter=name=projects/%v/locations/%v/privateConnections/%v", g.Project, g.Region, privateConnectionName),
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
	profileName := fmt.Sprintf("postgres-%v", g.DB)
	exists, err := g.profileExists(ctx, profileName)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	host, port, err := g.getProxyIPAndPort(ctx)
	if err != nil {
		return err
	}

	g.log.Infof("Creating Datastream postgres profile...")
	err = g.performRequest(ctx, []string{
		"datastream",
		"connection-profiles",
		"create",
		profileName,
		fmt.Sprintf("--display-name=postgres-%v", g.DB),
		"--type=postgresql",
		fmt.Sprintf("--location=%v", g.Region),
		fmt.Sprintf("--private-connection=%v", privateConnectionName),
		fmt.Sprintf("--postgresql-database=%v", g.DB),
		fmt.Sprintf("--postgresql-hostname=%v", host),
		fmt.Sprintf("--postgresql-username=%v", g.User),
		fmt.Sprintf("--postgresql-password=%v", g.Password),
		fmt.Sprintf("--postgresql-port=%v", port),
	}, nil)
	if err != nil {
		return err
	}

	// creation of datastream profiles fails silently, so we must check whether the resource was created
	exists, err = g.profileExists(ctx, profileName)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("unable to create datastream profile %v", profileName)
	}

	return nil
}

func (g *Google) createBigqueryProfile(ctx context.Context) error {
	profileName := fmt.Sprintf("bigquery-%v", g.DB)
	exists, err := g.profileExists(ctx, profileName)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	g.log.Infof("Creating Datastream Bigquery profile...")
	err = g.performRequest(ctx, []string{
		"datastream",
		"connection-profiles",
		"create",
		profileName,
		fmt.Sprintf("--display-name=bigquery-%v", g.DB),
		"--type=bigquery",
		fmt.Sprintf("--location=%v", g.Region),
	}, nil)
	if err != nil {
		return err
	}

	// creation of datastream profiles fails silently, so we must check whether the resource was created
	exists, err = g.profileExists(ctx, profileName)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("unable to create datastream profile %v", profileName)
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
		fmt.Sprintf("--location=%v", g.Region),
	}, &profiles)
	if err != nil {
		return false, err
	}

	for _, p := range profiles {
		if p.Name == fmt.Sprintf("projects/%v/locations/%v/connectionProfiles/%v", g.Project, g.Region, profileName) {
			return true, nil
		}
	}

	return false, nil
}

func (g *Google) streamExists(ctx context.Context, streamName string) (bool, error) {
	type datastream struct {
		Name string `json:"name"`
	}
	datastreams := []*datastream{}

	err := g.performRequest(ctx, []string{
		"datastream",
		"streams",
		"list",
		fmt.Sprintf("--location=%v", g.Region),
	}, &datastreams)
	if err != nil {
		return false, err
	}

	for _, s := range datastreams {
		if s.Name == fmt.Sprintf("projects/%v/locations/%v/streams/%v", g.Project, g.Region, streamName) {
			return true, nil
		}
	}

	return false, nil
}
