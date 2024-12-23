package containers

import (
	"context"
	"fmt"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"
)

func Localstack(ctx context.Context, img string) (testcontainers.Container, string, error) {
	// https://golang.testcontainers.org/modules/localstack/
	container, err := localstack.Run(ctx, img)
	if err != nil {
		return nil, "", fmt.Errorf("localstack.Run: %w", err)
	}

	endpoint, err := container.PortEndpoint(ctx, "4566/tcp", "http")
	if err != nil {
		return nil, "", fmt.Errorf("container.PortEndpoint: %w", err)
	}

	return container, endpoint, nil
}
