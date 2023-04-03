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

	"github.com/navikt/nada-datastream/cmd"
	"github.com/sirupsen/logrus"
)

const (
	gcloudTimeout = 45 * time.Minute
)

type Google struct {
	log *logrus.Entry
	*cmd.Config
}

func New(log *logrus.Entry, cfg *cmd.Config) *Google {
	return &Google{
		log:    log,
		Config: cfg,
	}
}

func (g *Google) performRequest(ctx context.Context, args []string, out interface{}) error {
	if out == nil {
		out = []map[string]interface{}{}
	}

	args = append(args, fmt.Sprintf("--project=%v", g.Project))
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
