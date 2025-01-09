package main

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsSns "github.com/aws/aws-sdk-go-v2/service/sns"
	snsTypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/jackc/pgx/v5/pgxpool"
	outbox "github.com/nikolayk812/pgx-outbox"
	"github.com/nikolayk812/pgx-outbox/examples/01_sns/clients/sns"
	"github.com/nikolayk812/pgx-outbox/examples/01_sns/clients/tracing"
	outboxSns "github.com/nikolayk812/pgx-outbox/sns"
	"github.com/nikolayk812/pgx-outbox/types"
	"github.com/spf13/viper"
)

const (
	// Postgres
	defaultConnStr = "postgres://user:password@localhost:5432/dbname"
	outboxTable    = "outbox_messages"

	// Localstack
	region          = "eu-central-1"
	defaultEndpoint = "http://localhost:4566"

	// Tracing
	defaultTracingEndpoint = "localhost:4317"
	tracerName             = "pgx-outbox/forwarder"

	defaultInterval = 5 * time.Second
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

	tracingEndpoint := cmp.Or(viper.GetString("TRACING_ENDPOINT"), defaultTracingEndpoint)
	dbURL := cmp.Or(viper.GetString("DB_URL"), defaultConnStr)
	localstackEndpoint := cmp.Or(viper.GetString("LOCALSTACK_ENDPOINT"), defaultEndpoint)
	interval := cmp.Or(viper.GetDuration("FORWARDER_INTERVAL"), defaultInterval)

	ctx := context.Background()

	shutdownTracer, err := tracing.InitGrpcTracer(ctx, tracingEndpoint, tracerName)
	if err != nil {
		gErr = fmt.Errorf("tracing.InitGrpcTracer: %w", err)
		return
	}
	defer shutdownTracer()

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

	slog.Info("Forwarder Ready") // integration test waits for this message

	for {
		stats, err := forwarder.Forward(ctx, 10)
		if err != nil {
			gErr = fmt.Errorf("forwarder.Forward: %w", err)
			return
		}

		slog.Info("forwarded", "stats", stats)

		time.Sleep(interval)
	}
}

type simpleTransformer struct{}

func (t simpleTransformer) Transform(_ context.Context, message types.Message) (*awsSns.PublishInput, error) {
	// 000000000000 is the AWS account ID for Localstack.
	topicARN := fmt.Sprintf("arn:aws:sns:%s:000000000000:%s", region, message.Topic)

	input := &awsSns.PublishInput{
		Message:  aws.String(string(message.Payload)),
		TopicArn: &topicARN,
	}

	if len(message.Metadata) > 0 {
		input.MessageAttributes = make(map[string]snsTypes.MessageAttributeValue)
		for k, v := range message.Metadata {
			input.MessageAttributes[k] = snsTypes.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String(v),
			}
		}
	}

	return input, nil
}
