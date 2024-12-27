package main

import (
	"context"
	"fmt"
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
	connStr     = "postgres://user:password@localhost:5432/dbname"
	outboxTable = "outbox_messages"

	// Localstack
	region   = "eu-central-1"
	endpoint = "http://localhost:4566"
	topic    = "topic1"
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

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		gErr = fmt.Errorf("pgxpool.New: %w", err)
		return
	}

	awsSnsCli, err := sns.NewAwsClient(ctx, region, endpoint)
	if err != nil {
		gErr = fmt.Errorf("sns.NewAwsClient: %w", err)
		return
	}

	snsCli, err := sns.New(awsSnsCli)
	if err != nil {
		gErr = fmt.Errorf("sns.New: %w", err)
		return
	}

	if _, err := snsCli.CreateTopic(ctx, topic); err != nil {
		gErr = fmt.Errorf("snsCli.CreateTopic: %w", err)
		return
	}

	reader, err := outbox.NewReader(pool, outboxTable)
	if err != nil {
		gErr = fmt.Errorf("outbox.NewReader: %w", err)
		return
	}

	publisher, err := outboxSns.NewPublisher(awsSnsCli, simpleTransformer{})
	if err != nil {
		gErr = fmt.Errorf("sns.NewPublisher: %w", err)
		return
	}

	forwarder, err := outbox.NewForwarder(reader, publisher)
	if err != nil {
		gErr = fmt.Errorf("outbox.NewForwarder: %w", err)
		return
	}

	for {
		stats, err := forwarder.Forward(ctx, types.MessageFilter{}, 10)
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
	topicARN := fmt.Sprintf("arn:aws:sns:%s:000000000000:%s", region, message.Topic)

	return &awsSns.PublishInput{
		Message:  aws.String(string(message.Payload)),
		TopicArn: &topicARN,
	}, nil
}
