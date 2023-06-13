package google

import (
	"context"
	"fmt"
)

const (
	DATASTREAM          = "datastream"
	SOURCE_PROFILE      = "source connection profile"
	DESTINATION_PROFILE = "destination connection profile"
	SERVICE_ACCOUNT     = "service account"
	FIREWALLRULE        = "firewall rule"
	PRIVATE_CONN        = "private connection"
	VPC                 = "VPC"
	SQL_PROXY           = "cloud sql proxy"
	DATASTREAM_API      = "datastream API"
)

var deleteResourceFunc map[string]func(Google, context.Context, string) error = map[string]func(Google, context.Context, string) error{
	DATASTREAM:          Google.deleteStream,
	SOURCE_PROFILE:      Google.deletePostgresProfile,
	DESTINATION_PROFILE: Google.deleteBigqueryProfile,
	SQL_PROXY:           Google.deleteCloudSQLProxy,
	SERVICE_ACCOUNT:     Google.deleteSA,
	FIREWALLRULE:        Google.deleteDatastreamFirewallRule,
	PRIVATE_CONN:        Google.deletePrivateConnection,
	VPC:                 Google.deleteVPC,
	DATASTREAM_API:      Google.disableDatastreamAPIs,
}

var createResourceFunc map[string]func(Google, context.Context, string) error = map[string]func(Google, context.Context, string) error{
	DATASTREAM:          Google.createStream,
	SOURCE_PROFILE:      Google.createPostgresProfile,
	DESTINATION_PROFILE: Google.createBigqueryProfile,
	SQL_PROXY:           Google.createCloudSQLProxy,
	SERVICE_ACCOUNT:     Google.createSAAndGrantRoles,
	FIREWALLRULE:        Google.createDatastreamFirewallRule,
	PRIVATE_CONN:        Google.createPrivateConnection,
	VPC:                 Google.createVPC,
}

var isSharedGlobalResource map[string]bool = map[string]bool{
	DATASTREAM:          false,
	SOURCE_PROFILE:      false,
	DESTINATION_PROFILE: false,
	SQL_PROXY:           false,
	SERVICE_ACCOUNT:     true,
	FIREWALLRULE:        true,
	PRIVATE_CONN:        true,
	VPC:                 true,
	DATASTREAM_API:      true,
}

var checkExistenceFunc map[string]func(Google, context.Context, string) (bool, error) = map[string]func(Google, context.Context, string) (bool, error){
	DATASTREAM:          Google.streamExists,
	SOURCE_PROFILE:      Google.profileExists,
	DESTINATION_PROFILE: Google.profileExists,
	SQL_PROXY:           Google.cloudSQLProxyExists,
	SERVICE_ACCOUNT:     Google.saExists,
	FIREWALLRULE:        Google.datastreamFirewallRuleExists,
	PRIVATE_CONN:        Google.privateConnectionExists,
	VPC:                 Google.vpcExists,
	DATASTREAM_API:      func(g Google, ctx context.Context, s string) (bool, error) { return false, nil },
}

var generateNameFunc map[string]func(*Google) string = map[string]func(*Google) string{
	DATASTREAM: func(g *Google) string {
		return fmt.Sprintf("postgres-%v-bigquery", g.DB)
	},
	SOURCE_PROFILE: func(g *Google) string {
		return fmt.Sprintf("postgres-%v", g.DB)
	},
	DESTINATION_PROFILE: func(g *Google) string {
		return fmt.Sprintf("bigquery-%v", g.DB)
	},
	SQL_PROXY: func(g *Google) string {
		return proxyVMNamePrefix + g.DB
	},
	SERVICE_ACCOUNT: func(g *Google) string {
		return serviceAccountName
	},
	FIREWALLRULE: func(g *Google) string {
		return firewallRuleName
	},
	PRIVATE_CONN: func(g *Google) string {
		return privateConnectionName
	},
	VPC: func(g *Google) string {
		return vpcName
	},
	DATASTREAM_API: func(g *Google) string {
		return "datastream.googleapis.com"
	},
}

func (g *Google) DeleteResources(ctx context.Context) error {
	otherStream, err := g.anyOtherStreamExistis(ctx)
	if err != nil {
		return err
	}

	resources := []string{
		DATASTREAM,
		SOURCE_PROFILE,
		DESTINATION_PROFILE,
		SERVICE_ACCOUNT,
		FIREWALLRULE,
		PRIVATE_CONN,
		VPC,
		SQL_PROXY,
		DATASTREAM_API,
	}

	for i, k := range resources {

		if isSharedGlobalResource[k] && otherStream {
			g.log.Info(fmt.Sprintf("Other datastream(s) depends on resource [%v], skip deletion", k))
			continue
		}

		exist, err := checkExistenceFunc[k](*g, ctx, generateNameFunc[k](g))
		if err != nil {
			g.log.Info(fmt.Sprintf("Terminated on error, following resource(s) has not been cleaned up: %v", resources[i:]))
			return err
		}

		if exist {
			err = deleteResourceFunc[k](*g, ctx, generateNameFunc[k](g))
			if err != nil {
				g.log.Info(fmt.Sprintf("Terminated on error, following resource(s) has not been cleaned up: %v", resources[i:]))
				return err
			}
		} else {
			g.log.Info(fmt.Sprintf("Resource [%v] does not exist, skip deletion", k))
		}
	}
	return nil
}

func (g *Google) CreateResources(ctx context.Context) error {
	err := g.EnableAPIs(ctx)
	if err != nil {
		return err
	}

	resources := []string{
		VPC,
		SERVICE_ACCOUNT,
		SQL_PROXY,
		PRIVATE_CONN,
		FIREWALLRULE,
		SOURCE_PROFILE,
		DESTINATION_PROFILE,
		DATASTREAM,
	}

	for i, k := range resources {
		exist, err := checkExistenceFunc[k](*g, ctx, generateNameFunc[k](g))
		if err != nil {
			if i != 0 {
				g.log.Info(fmt.Sprintf("Teminated on error, following resource(s) may need to be cleaned up: %v", resources[0:i]))
				//Should we do an auto cleanup?
			}
			return err
		}
		if exist {
			g.log.Info(fmt.Sprintf("Resource [%v] exists, skip creation", k))
			continue
		}
		err = createResourceFunc[k](*g, ctx, generateNameFunc[k](g))
		if err != nil {
			if i != 0 {
				g.log.Info(fmt.Sprintf("Teminated on error, following resource(s) may need to be cleaned up: %v", resources[0:i]))
			}
			return err
		}
	}
	return nil
}
