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
	port     string
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
			instance: "datastream",
			region:   "europe-north1",
			db:       "datastream",      // hent fra secret i clusteret
			user:     "datastream_read", // hent fra secret i clusteret
			password: "<replace me>",    // hent fra secret i clusteret
			port:     "5432",
		},
	}
}

func (g *Google) performRequest(ctx context.Context, args []string, out any) error {
	if out == nil {
		out = []map[string]any{}
	}

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
		return err
	}

	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		return err
	}

	return nil
}
