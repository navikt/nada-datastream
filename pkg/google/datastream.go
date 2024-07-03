package google

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

const (
	privateConnectionName = "datastream-connection"
	datastreamSubnet      = "10.2.0.0/29" // arbitrary
	firewallRuleName      = "allow-datastream-cloudsql-proxy"
)

func (g Google) createStream(ctx context.Context, streamName string) error {
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

func (g Google) createPrivateConnection(ctx context.Context, connection string) error {
	g.log.Infof("Creating Datastream private connection...")
	err := g.performRequest(ctx, []string{
		"datastream",
		"private-connections",
		"create",
		connection,
		fmt.Sprintf("--display-name=%v", privateConnectionName),
		fmt.Sprintf("--vpc=%v", vpcName),
		fmt.Sprintf("--subnet=%v", datastreamSubnet),
		fmt.Sprintf("--location=%v", g.Region),
	}, nil)
	if err != nil {
		return err
	}

	if err := g.waitForPrivateConnectionUp(ctx); err != nil {
		return err
	}

	return nil
}

func (g Google) privateConnectionExists(ctx context.Context, privateConnection string) (bool, error) {
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
		if c.Name == fmt.Sprintf("projects/%v/locations/%v/privateConnections/%v", g.Project, g.Region, privateConnection) {
			return true, nil
		}
	}

	return false, nil
}

func (g Google) createDatastreamFirewallRule(ctx context.Context, firewallRuleName string) error {
	err := g.performRequest(ctx, []string{
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

func (g Google) datastreamFirewallRuleExists(ctx context.Context, firewallRule string) (bool, error) {
	type firewallRuleType struct {
		Name string `json:"name"`
	}
	firewallRules := []*firewallRuleType{}

	err := g.performRequest(ctx, []string{
		"compute",
		"firewall-rules",
		"list",
	}, &firewallRules)
	if err != nil {
		return false, err
	}

	for _, r := range firewallRules {
		if r.Name == firewallRule {
			return true, nil
		}
	}

	return false, nil
}

func (g Google) createPostgresProfile(ctx context.Context, profileName string) error {
	host, err := g.getProxyIP(ctx, generateNameFunc[SQL_PROXY](&g))
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
	exists, err := g.profileExists(ctx, profileName)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("unable to create datastream profile %v", profileName)
	}

	return nil
}

func (g Google) createBigqueryProfile(ctx context.Context, profileName string) error {
	g.log.Infof("Creating Datastream Bigquery profile...")
	err := g.performRequest(ctx, []string{
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
	exists, err := g.profileExists(ctx, profileName)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("unable to create datastream profile %v", profileName)
	}

	return nil
}

func (g Google) profileExists(ctx context.Context, profileName string) (bool, error) {
	type profile struct {
		Name        string `json:"name"`
		DisplayName string `json:"display_name"`
	}
	numReadyCheckRetries := 5

OUTER:
	for i := 0; i < numReadyCheckRetries; i++ {
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
				// Need this check as the connection profile is not necessarily ready when the list command returns a connection profile.
				// The next step (create datastream) fails if the connection profiles are not ready.
				// When the display name field is set to an non empty string, the connection profile is ready.
				if p.DisplayName == "" {
					g.log.Infof("Waiting for connection profile %v ready", profileName)
					time.Sleep(30 * time.Second)
					continue OUTER
				}
				return true, nil
			}
		}

		return false, nil
	}

	return false, nil
}

type datastream struct {
	Name string `json:"name"`
}

func (g Google) streamExists(ctx context.Context, streamName string) (bool, error) {
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

func (g *Google) anyOtherStreamExistis(ctx context.Context) (bool, error) {
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

	otherStream := 0
	stream := generateNameFunc[DATASTREAM](g)
	for _, ds := range datastreams {
		if ds.Name != stream {
			otherStream++
		}
	}
	return otherStream > 0, nil
}

func (g *Google) createPostgresStreamConfig(ctx context.Context) (string, error) {
	cfg := map[string]any{}
	cfg["replicationSlot"] = g.ReplicationSlot
	cfg["publication"] = g.Publication

	if len(g.IncludeTables) > 0 {
		tables := []map[string]string{}
		for _, t := range g.IncludeTables {
			tables = append(tables, map[string]string{"table": t})
		}
		cfg["includeObjects"] = map[string]any{
			"postgresqlSchemas": []map[string]any{
				{
					"schema":           "public",
					"postgresqlTables": tables,
				},
			},
		}
	} else if len(g.ExcludeTables) > 0 {
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
	datasetID := "datastream_" + strings.ReplaceAll(g.DB, "-", "_")
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
		"dataFreshness": fmt.Sprintf("%ds", g.DataFreshness),
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

func (g Google) datasetExists(ctx context.Context, datasetID string) (bool, error) {
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

func (g Google) deleteStream(ctx context.Context, streamName string) error {
	streamExist, err := g.streamExists(ctx, streamName)
	if err != nil {
		return err
	}
	if !streamExist {
		g.log.Info("Datastream does not exist, so skip deletion")
		return nil
	}

	g.log.Info("Deleting datastream...")
	return g.performRequest(ctx, []string{
		"datastream",
		"streams",
		"delete",
		streamName,
		fmt.Sprintf("--location=%v", g.Region),
		"--quiet",
	}, nil)
}

func (g Google) deletePostgresProfile(ctx context.Context, profileName string) error {
	profileExist, err := g.profileExists(ctx, profileName)
	if err != nil {
		return err
	}
	if !profileExist {
		g.log.Info(fmt.Sprintf("Connection profile %v does not exist, so skip deletion", profileName))
		return nil
	}

	g.log.Infof("Deleting Datastream postgres profile...")
	return g.performRequest(ctx, []string{
		"datastream",
		"connection-profiles",
		"delete",
		profileName,
		fmt.Sprintf("--location=%v", g.Region),
		"--quiet",
	}, nil)
}

func (g Google) deleteBigqueryProfile(ctx context.Context, profileName string) error {
	profileExist, err := g.profileExists(ctx, profileName)
	if err != nil {
		return err
	}
	if !profileExist {
		g.log.Info(fmt.Sprintf("Connection profile %v does not exist, so skip deletion", profileName))
		return nil
	}

	g.log.Infof("Deleting Datastream Bigquery profile...")
	return g.performRequest(ctx, []string{
		"datastream",
		"connection-profiles",
		"delete",
		profileName,
		fmt.Sprintf("--location=%v", g.Region),
		"--quiet",
	}, nil)
}

func (g Google) deletePrivateConnection(ctx context.Context, privateConnection string) error {
	g.log.Infof("Deleting Datastream private connection...")
	return g.performRequest(ctx, []string{
		"datastream",
		"private-connections",
		"delete",
		privateConnection,
		fmt.Sprintf("--location=%v", g.Region),
		"--quiet",
		"--force",
	}, nil)
}

func (g Google) deleteDatastreamFirewallRule(ctx context.Context, firewallRule string) error {
	g.log.Infof("Deleting Datastream vpc firewall rule...")
	return g.performRequest(ctx, []string{
		"compute",
		"firewall-rules",
		"delete",
		firewallRule,
		"--quiet",
	}, nil)
}
