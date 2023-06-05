package cmd

type DBConfig struct {
	Project   string
	Region    string
	Instance  string
	DB        string
	User      string
	Password  string
	Port      string
	Namespace string
}

type Config struct {
	*DBConfig

	ExcludeTables   []string
	ReplicationSlot string
	Publication     string
}

const (
	Namespace           = "namespace"
	Context             = "context"
	ExcludeTables       = "exclude-tables"
	ReplicationSlotName = "replication-slot"
	PublicationName     = "publication-name"
)
