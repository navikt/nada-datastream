package cmd

type DBConfig struct {
	Project  string
	Region   string
	Instance string
	DB       string
	User     string
	Password string
	Port     string
}

type Config struct {
	*DBConfig

	CloudSQLPrivateIP bool
	ExcludeTables     []string
	ReplicationSlot   string
	Publication       string
}

const (
	Namespace            = "namespace"
	Context              = "context"
	ExcludeTables        = "exclude-tables"
	ReplicationSlotName  = "replication-slot"
	PublicationName      = "publication-name"
	UseCloudSQLPrivateIP = "cloudsql-private-ip"
)
