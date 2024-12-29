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

func Localstack(ctx context.Context, img, services string) (testcontainers.Container, string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, "", fmt.Errorf("os.Getwd: %w", err)
	}

	envCustomizer := EnvCustomizer{
		Services:   services,
		WorkingDir: cwd,
	}

	// https://golang.testcontainers.org/modules/localstack/
	cont, err := localstack.Run(ctx, img, envCustomizer)
	if err != nil {
		return nil, "", fmt.Errorf("localstack.Run: %w", err)
	}

	endpoint, err := cont.PortEndpoint(ctx, "4566/tcp", "http")
	if err != nil {
		return nil, "", fmt.Errorf("cont.PortEndpoint: %w", err)
	}

	return cont, endpoint, nil
}

type EnvCustomizer struct {
	Services   string
	WorkingDir string
}

func (e EnvCustomizer) Customize(req *testcontainers.GenericContainerRequest) error {
	req.Env = map[string]string{"SERVICES": e.Services}

	// This has to match the log message from the initialization script, otherwise "Ready." can be used.
	req.WaitingFor = wait.ForLog("LocalStack resources initialized successfully.")

	// Absolute path of a script at the host machine has to be provided.
	// Tests can be run from different directories, i.e. IDE from ./sns and `make test` from the root.
	// The script is located in ./internal/containers/localstack-init.sh
	dir := resolveDir(e.WorkingDir)
	absPath := filepath.Join(dir, "localstack-init.sh")

	req.Entrypoint = []string{"docker-entrypoint.sh"}
	req.HostConfigModifier = func(hostConfig *container.HostConfig) {
		// https://docs.localstack.cloud/references/init-hooks/
		hostConfig.Binds = append(hostConfig.Binds, fmt.Sprintf("%s:/etc/localstack/init/ready.d/localstack-init.sh", absPath))
	}

	return nil
}

func resolveDir(workingDir string) string {
	workingDir = strings.TrimSuffix(workingDir, "/sns")
	return fmt.Sprintf("%s/internal/containers/", workingDir)
}
