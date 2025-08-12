package containers

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/docker/docker/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"
	"github.com/testcontainers/testcontainers-go/wait"
)

func Localstack(ctx context.Context, img, services, network string) (testcontainers.Container, string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, "", fmt.Errorf("os.Getwd: %w", err)
	}

	cc := lsCustomizer{
		Services:   services,
		WorkingDir: cwd,
		Network:    network,
	}

	// https://golang.testcontainers.org/modules/localstack/
	cont, err := localstack.Run(ctx, img, cc)
	if err != nil {
		return nil, "", fmt.Errorf("localstack.Run: %w", err)
	}

	endpoint, err := cont.PortEndpoint(ctx, "4566/tcp", "http")
	if err != nil {
		return nil, "", fmt.Errorf("cont.PortEndpoint: %w", err)
	}

	return cont, endpoint, nil
}

type lsCustomizer struct {
	Services   string
	WorkingDir string
	Network    string
}

func (c lsCustomizer) Customize(req *testcontainers.GenericContainerRequest) error {
	req.Env = map[string]string{
		"SERVICES":               c.Services,
		"SKIP_SSL_CERT_DOWNLOAD": "true",
		"DISABLE_EVENTS":         "1",
	}

	// This has to match the log message from the initialization script, otherwise "Ready." can be used.
	req.WaitingFor = wait.ForLog("LocalStack resources initialized successfully.")

	// Absolute path of a script at the host machine has to be provided.
	// Tests can be run from different directories, i.e. IDE from ./sns and `make test` from the root.
	// The script is located in ./internal/containers/localstack-init.sh
	dir := c.resolveDir(c.WorkingDir)
	absPath := filepath.Join(dir, "localstack-init.sh")

	req.Entrypoint = []string{"docker-entrypoint.sh"}
	req.HostConfigModifier = func(hostConfig *container.HostConfig) {
		// https://docs.localstack.cloud/references/init-hooks/
		hostConfig.Binds = append(hostConfig.Binds, fmt.Sprintf("%s:/etc/localstack/init/ready.d/localstack-init.sh", absPath))
	}

	if c.Network != "" {
		req.Networks = []string{c.Network}
		req.NetworkAliases = map[string][]string{
			c.Network: {"localstack"},
		}
	}

	return nil
}

func (c lsCustomizer) resolveDir(workingDir string) string {
	token := projectName + "/" + projectName

	parts := strings.Split(workingDir, token)
	if len(parts) > 1 {
		// GitHub Actions case, i.e. /home/runner/work/pgx-outbox/pgx-outbox/writer_reader_test.go
		workingDir = parts[0] + token
	} else {
		// local machine case
		parts = strings.Split(workingDir, projectName)
		if len(parts) > 1 {
			workingDir = parts[0] + projectName
		}
	}

	return fmt.Sprintf("%s/internal/containers/", workingDir)
}
