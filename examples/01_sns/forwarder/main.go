package main

import (
	"cmp"
	"context"
	"fmt"
	"github.com/spf13/viper"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsSns "github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/jackc/pgx/v5/pgxpool"
	outbox "github.com/nikolayk812/pgx-outbox"
	"github.com/nikolayk812/pgx-outbox/examples/01_sns/clients/sns"
	outboxSns "github.com/nikolayk812/pgx-outbox/sns"
	"github.com/nikolayk812/pgx-outbox/types"
)

const (
	// Postgres
	defaultConnStr = "postgres://user:password@localhost:5432/dbname"
	outboxTable    = "outbox_messages"

	// Localstack
	region          = "eu-central-1"
	defaultEndpoint = "http://localhost:4566"
)

func main() {
	var gErr error

	defer func() {
		if gErr != nil {
			slog.Error("global error", "error", gErr)
			os.Exit(1)
		}

		os.Exit(0)
	}()

	viper.AutomaticEnv()

	dbURL := cmp.Or(viper.GetString("DB_URL"), defaultConnStr)
	localstackEndpoint := cmp.Or(viper.GetString("LOCALSTACK_ENDPOINT"), defaultEndpoint)

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		gErr = fmt.Errorf("pgxpool.New: %w", err)
		return
	}

	snsCli, err := sns.NewAwsClient(ctx, region, localstackEndpoint)
	if err != nil {
		gErr = fmt.Errorf("sns.NewAwsClient: %w", err)
		return
	}

	publisher, err := outboxSns.NewPublisher(snsCli, simpleTransformer{})
	if err != nil {
		gErr = fmt.Errorf("sns.NewPublisher: %w", err)
		return
	}

	forwarder, err := outbox.NewForwarderFromPool(outboxTable, pool, publisher)
	if err != nil {
		gErr = fmt.Errorf("outbox.NewForwarder: %w", err)
		return
	}

	slog.Info("Forwarder Ready")

	for {
		stats, err := forwarder.Forward(ctx, 10)
		if err != nil {
			gErr = fmt.Errorf("forwarder.Forward: %w", err)
			return
		}

		slog.Info("forwarded", "stats", stats)

		time.Sleep(5 * time.Second)
	}
}

type simpleTransformer struct{}

func (t simpleTransformer) Transform(message types.Message) (*awsSns.PublishInput, error) {
	// 000000000000 is the AWS account ID for Localstack.
	topicARN := fmt.Sprintf("arn:aws:sns:%s:000000000000:%s", region, message.Topic)

	return &awsSns.PublishInput{
		Message:  aws.String(string(message.Payload)),
		TopicArn: &topicARN,
	}, nil
}
