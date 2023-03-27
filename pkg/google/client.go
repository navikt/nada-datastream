package google

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	gcloudTimeout = 25 * time.Minute
)

type PostgresDB struct {
	instance string
	region   string
	db       string
	user     string
	password string
}

type Google struct {
	PostgresDB

	project string
	log     *logrus.Entry
}

func New(log *logrus.Entry) *Google {
	return &Google{
		log:     log,
		project: "nada-dev-db2e",
		PostgresDB: PostgresDB{
			instance: "ds-test",
			region:   "europe-north1",
			db:       "ds",
			user:     "datastream",
			password: "",
		},
	}
}

func (g *Google) performRequest(ctx context.Context, args []string) ([]map[string]any, error) {
	args = append(args, fmt.Sprintf("--project=%v", g.project))
	args = append(args, "--format=json")

	ctxWithTimeout, cancel := context.WithTimeout(ctx, gcloudTimeout)
	cmd := exec.CommandContext(
		ctxWithTimeout,
		"gcloud",
		args...,
	)
	defer cancel()

	buf := &bytes.Buffer{}
	cmd.Stdout = buf
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		io.Copy(os.Stdout, buf)
		return nil, err
	}

	var out []map[string]any
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		return nil, err
	}

	return out, nil
}
