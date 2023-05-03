package google

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
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
		return nil
	}

	pgConfig, err := g.createPostgresStreamConfig(ctx)
	if err != nil {
		return err
	}
	defer deleteTempFile(pgConfig)

	bqConfig, err := g.createBigQueryStreamConfig(ctx)
	if err != nil {
		return err
	}
	defer deleteTempFile(bqConfig)

	g.log.Info("Creating datastream...")
	err = g.performRequest(ctx, []string{
		"datastream",
		"streams",
		"create",
		streamName,
		fmt.Sprintf("--display-name=%v", streamName),
		fmt.Sprintf("--location=%v", g.Region),
		fmt.Sprintf("--source=projects/%v/locations/%v/connectionProfiles/postgres-%v", g.Project, g.Region, g.DB),
		fmt.Sprintf("--postgresql-source-config=%v", pgConfig),
		fmt.Sprintf("--destination=projects/%v/locations/%v/connectionProfiles/bigquery-%v", g.Project, g.Region, g.DB),
		fmt.Sprintf("--bigquery-destination-config=%v", bqConfig),
		"--backfill-all",
		"--labels=created-by=nada",
	}, nil)
	if err != nil {
		return err
	}

	g.log.Infof("Gå til https://console.cloud.google.com/datastream/streams?referrer=search&project=%v for å aktivere streamen %v", g.Project, streamName)
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

	host, err := g.getProxyIP(ctx, proxyVMNamePrefix+g.DB)
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
		"--postgresql-port=5432",
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

func (g *Google) createPostgresStreamConfig(ctx context.Context) (string, error) {
	cfg := map[string]any{}
	cfg["replicationSlot"] = g.ReplicationSlot
	cfg["publication"] = g.Publication

	if len(g.ExcludeTables) > 0 {
		tables := []map[string]string{}
		for _, t := range g.ExcludeTables {
			tables = append(tables, map[string]string{"table": t})
		}
		cfg["excludeObjects"] = map[string]any{
			"postgresqlSchemas": []map[string]any{
				{
					"schema":           "public",
					"postgresqlTables": tables,
				},
			},
		}
	}

	file, err := ioutil.TempFile("", "ds-pg-config")
	if err != nil {
		return "", err
	}

	cfgBytes, err := json.Marshal(cfg)
	if err != nil {
		return "", err
	}
	_, err = file.Write(cfgBytes)
	if err != nil {
		return "", err
	}

	return file.Name(), nil
}

func (g *Google) createBigQueryStreamConfig(ctx context.Context) (string, error) {
	datasetID := "datastream_" + g.DB
	exists, err := g.datasetExists(ctx, datasetID)
	if err != nil {
		return "", err
	}
	if !exists {
		if err := g.createDataset(ctx, datasetID); err != nil {
			return "", err
		}
	}

	cfg := map[string]any{
		"singleTargetDataset": map[string]string{
			"datasetId": fmt.Sprintf("%v:%v", g.Project, datasetID),
		},
		"dataFreshness": "900s",
	}

	file, err := ioutil.TempFile("", "ds-bq-config")
	if err != nil {
		return "", err
	}

	cfgBytes, err := json.Marshal(cfg)
	if err != nil {
		return "", err
	}
	_, err = file.Write(cfgBytes)
	if err != nil {
		return "", err
	}

	return file.Name(), nil
}

func (g *Google) datasetExists(ctx context.Context, datasetID string) (bool, error) {
	client, err := bigquery.NewClient(ctx, g.Project)
	if err != nil {
		return false, err
	}
	defer client.Close()

	datasets := client.Datasets(ctx)
	if err != nil {
		return false, err
	}

	for {
		ds, err := datasets.Next()
		if err != nil {
			if errors.Is(err, iterator.Done) {
				return false, nil
			}
			return false, err
		}

		if ds.DatasetID == datasetID {
			return true, nil
		}
	}
}

func (g *Google) createDataset(ctx context.Context, datasetID string) error {
	client, err := bigquery.NewClient(ctx, g.Project)
	if err != nil {
		return err
	}
	defer client.Close()

	return client.Dataset(datasetID).Create(ctx, &bigquery.DatasetMetadata{
		Location: g.Region,
	})
}

func deleteTempFile(file string) {
	if err := os.RemoveAll(file); err != nil {
		panic(err)
	}
}
