package containers

import (
	"context"
	"fmt"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func Postgres(ctx context.Context, img string) (testcontainers.Container, string, error) {
	container, err := postgres.Run(ctx, img,
		postgres.WithDatabase("test-db"),
		postgres.BasicWaitStrategies(),
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
