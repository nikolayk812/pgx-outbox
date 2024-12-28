package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/nikolayk812/pgx-outbox/examples/01_sns/clients/sqs"
)

const (
	region   = "eu-central-1"
	endpoint = "http://localhost:4566"
	topic    = "topic1"
	queue    = "queue1"
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

	awsSqsCli, err := sqs.NewAwsClient(ctx, region, endpoint)
	if err != nil {
		gErr = fmt.Errorf("sqs.NewAwsClient: %w", err)
		return
	}

	sqsCli, err := sqs.New(awsSqsCli)
	if err != nil {
		gErr = fmt.Errorf("sqs.New: %w", err)
		return
	}

	queueURL, err := sqsCli.GetQueueURL(ctx, queue)
	if err != nil {
		gErr = fmt.Errorf("sqsCli.GetQueueURL: %w", err)
		return
	}

	for {
		sqsMessage, err := sqsCli.ReadOneFromSQS(ctx, queueURL, time.Second)
		if err != nil {
			switch {
			case errors.Is(err, context.DeadlineExceeded):
				slog.Debug("no messages")
				continue
			default:
				gErr = fmt.Errorf("sqsCli.ReadOneFromSQS: %w", err)
				return
			}
		}

		payload, err := sqsCli.ExtractOutboxPayload(sqsMessage)
		if err != nil {
			slog.Error(
				"sqsCli.ExtractOutboxPayload",
				"messageId", deref(sqsMessage.MessageId),
				"error", err,
			)
			continue
		}

		pretty, err := prettyJson(payload)
		if err != nil {
			slog.Error(
				"prettyJson",
				"messageId", deref(sqsMessage.MessageId),
				"error", err,
			)
			continue
		}

		// slog would escape the json string
		log.Printf("Message received:\n%s", pretty)
	}
}

func deref[T any](v *T) T {
	if v == nil {
		return *new(T)
	}
	return *v
}

func prettyJson(jsonData []byte) (string, error) {
	indentedData, err := json.MarshalIndent(json.RawMessage(jsonData), "", "  ")
	if err != nil {
		return "", fmt.Errorf("json.MarshalIndent: %w", err)
	}

	return string(indentedData), nil
}
