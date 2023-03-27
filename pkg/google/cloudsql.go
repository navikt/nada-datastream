package google

import (
	"context"
	"fmt"
)

func (g *Google) PatchCloudSQLInstance(ctx context.Context) error {
	g.log.Infof("Patching SQL instance...")
	_, err := g.performRequest(ctx, []string{
		"sql",
		"instances",
		"patch",
		g.instance,
		fmt.Sprintf("--network=%v", vpcName),
		"--database-flags=cloudsql.logical_decoding=On",
	})
	if err != nil {
		g.log.WithError(err).Errorf("patching sql instance %v", g.instance)
		return err
	}
	g.log.Infof("Done")

	return nil
}
