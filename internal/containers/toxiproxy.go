package containers

import (
	"context"
	"fmt"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func ToxiProxy(ctx context.Context, img, network string) (testcontainers.Container, string, error) {
	// https://golang.testcontainers.org/examples/toxiproxy/
	req := testcontainers.ContainerRequest{
		Image:        img,
		ExposedPorts: []string{"8474/tcp", "8666/tcp"},
		WaitingFor:   wait.ForHTTP("/version").WithPort("8474/tcp"),
		Networks: []string{
			network,
		},
		NetworkAliases: map[string][]string{
			network: {"toxiproxy"},
		},
	}

	cont, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, "", fmt.Errorf("tc.GenericContainer: %w", err)
	}

	endpoint, err := cont.PortEndpoint(ctx, "8474/tcp", "http")
	if err != nil {
		return nil, "", fmt.Errorf("cont.PortEndpoint: %w", err)
	}

	return cont, endpoint, nil
}
