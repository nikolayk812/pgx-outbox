package sns

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nikolayk812/pgx-outbox/internal/containers"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcNetwork "github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

const EventTypeUserCreated = "user_created" // has to match the one in writer/mapper.go

var ctx = context.Background()

// TestIntegration is an integration test that starts Postgres, Localstack, Writer, Forwarder, and SQS-Reader in containers.
// `make docker-build` target is required before running this test.
func TestIntegration(t *testing.T) {
	require.NoError(t, os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true"))

	// Create Docker network so all containers can communicate using aliases
	network, err := tcNetwork.New(ctx)
	require.NoError(t, err)
	testcontainers.CleanupNetwork(t, network)

	// Start Postgres container, ignore the endpoint at host machine, use the network alias
	pgContainer, _, err := containers.Postgres(ctx, "postgres:17.2-alpine", network.Name)
	require.NoError(t, err)
	testcontainers.CleanupContainer(t, pgContainer)
	pgConnStr := "postgres://postgres:postgres@postgres:5432/test-db"

	// Start Localstack container, ignore the endpoint at host machine, use the network alias
	localstack, _, err := containers.Localstack(ctx, "localstack/localstack:4.1.1", "sns,sqs", network.Name)
	require.NoError(t, err)
	testcontainers.CleanupContainer(t, localstack)
	localstackEndpoint := "http://localstack:4566"

	goContainer(t, "writer", pgConnStr, localstackEndpoint, network.Name, wait.NewLogStrategy("Writer Ready"))

	goContainer(t, "forwarder", pgConnStr, localstackEndpoint, network.Name, wait.NewLogStrategy("Forwarder Ready"))

	// wait strategy verifies that the messages are delivered from the Writer to the SQS-Reader end-to-end
	waitStrategy := wait.NewLogStrategy(EventTypeUserCreated).WithOccurrence(7).WithStartupTimeout(time.Second * 2)
	goContainer(t, "sqs-reader", pgConnStr, localstackEndpoint, network.Name, waitStrategy)
}

func goContainer(t *testing.T, imageSuffix, pgConnStr, localstackEndpoint, networkName string, logStrategy *wait.LogStrategy) testcontainers.Container {
	image := fmt.Sprintf("nikolayk812/pgx-outbox-%s", imageSuffix)

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:      image,
			WaitingFor: logStrategy,
			Env: map[string]string{
				"DB_URL":              pgConnStr,
				"LOCALSTACK_ENDPOINT": localstackEndpoint,
				"WRITER_INTERVAL":     "100ms",
				"FORWARDER_INTERVAL":  "200ms",
				"TRACING_ENDPOINT":    "off",
			},
			Networks: []string{networkName},
		},
		Started: true,
	})
	require.NoError(t, err)

	testcontainers.CleanupContainer(t, container)

	return container
}
