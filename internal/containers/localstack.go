package containers

import (
	"context"
	"fmt"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"
	"github.com/testcontainers/testcontainers-go/wait"
)

func Localstack(ctx context.Context, img string) (testcontainers.Container, string, error) {
	// https://golang.testcontainers.org/modules/localstack/

	envCustomizer := EnvCustomizer{}

	container, err := localstack.Run(ctx, img, envCustomizer)
	if err != nil {
		return nil, "", fmt.Errorf("localstack.Run: %w", err)
	}

	endpoint, err := container.PortEndpoint(ctx, "4566/tcp", "http")
	if err != nil {
		return nil, "", fmt.Errorf("container.PortEndpoint: %w", err)
	}

	return container, endpoint, nil
}

// EnvCustomizer is a customizer to set environment variables.
type EnvCustomizer struct{}

// Customize sets the environment variables in the container request.
func (e EnvCustomizer) Customize(req *testcontainers.GenericContainerRequest) error {
	req.Env = map[string]string{"SERVICES": "sns,sqs"}
	req.WaitingFor = wait.ForLog("Ready.")

	return nil
}
