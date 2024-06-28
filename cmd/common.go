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

	ExcludeTables   []string
	IncludeTables   []string
	ReplicationSlot string
	Publication     string
	DataFreshness   int
}

const (
	Namespace           = "namespace"
	Context             = "context"
	IncludeTables       = "include-tables"
	ExcludeTables       = "exclude-tables"
	ReplicationSlotName = "replication-slot"
	PublicationName     = "publication-name"
	DataFreshness       = "dataFreshness"
)
