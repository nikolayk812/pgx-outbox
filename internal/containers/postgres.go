package containers

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

const projectName = "pgx-outbox"

func Postgres(ctx context.Context, img, network string) (testcontainers.Container, string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, "", fmt.Errorf("os.Getwd: %w", err)
	}

	cc := pgCustomizer{
		Network:    network,
		WorkingDir: cwd,
	}

	container, err := postgres.Run(ctx, img,
		postgres.WithDatabase("test-db"),
		postgres.BasicWaitStrategies(),
		postgres.WithInitScripts(cc.initScripts()...),
		cc,
	)
	if err != nil {
		return nil, "", fmt.Errorf("postgres.Run: %w", err)
	}

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, "", fmt.Errorf("pc.ConnectionString: %w", err)
	}

	return container, connStr, nil
}

type pgCustomizer struct {
	WorkingDir string
	Network    string
}

func (c pgCustomizer) Customize(req *testcontainers.GenericContainerRequest) error {
	if c.Network != "" {
		req.Networks = []string{c.Network}
		req.NetworkAliases = map[string][]string{
			c.Network: {"postgres"},
		}
	}

	return nil
}

func (c pgCustomizer) resolveDir(workingDir string) string {
	// trim everything after /pgx-outbox
	parts := strings.Split(workingDir, projectName)
	if len(parts) > 1 {
		workingDir = parts[0] + projectName
	}

	return fmt.Sprintf("%s/internal/sql/", workingDir)
}

func (c pgCustomizer) initScripts() []string {
	dir := c.resolveDir(c.WorkingDir)
	return []string{
		filepath.Join(dir, "01_outbox_messages.up.sql"),
		filepath.Join(dir, "02_users.up.sql"),
	}
}
