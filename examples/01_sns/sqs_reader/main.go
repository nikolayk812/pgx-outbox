package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/nikolayk812/pgx-outbox/examples/01_sns/clients/sqs"
	"github.com/nikolayk812/pgx-outbox/examples/01_sns/clients/tracing"
	outbox "github.com/nikolayk812/pgx-outbox/types"
	"github.com/spf13/viper"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	region          = "eu-central-1"
	defaultEndpoint = "http://localhost:4566"
	queue           = "queue1"

	defaultTracingEndpoint = "localhost:4317"
	tracerName             = "pgx-outbox/sqs-reader"
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
	localstackEndpoint := cmp.Or(viper.GetString("LOCALSTACK_ENDPOINT"), defaultEndpoint)

	ctx := context.Background()

	shutdownTracer, err := tracing.InitGrpcTracer(ctx, tracingEndpoint, tracerName)
	if err != nil {
		gErr = fmt.Errorf("tracing.InitGrpcTracer: %w", err)
		return
	}
	defer shutdownTracer()

	awsSqsCli, err := sqs.NewAwsClient(ctx, region, localstackEndpoint)
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

	slog.Info("SQS-Reader Ready")

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

		outboxMessage, err := sqsCli.ToOutboxMessage(sqsMessage)
		if err != nil {
			slog.Error(
				"sqsCli.ToOutboxMessage",
				"messageId", deref(sqsMessage.MessageId),
				"error", err,
			)
			continue
		}

		ctx = tracing.ChildContext(ctx, outboxMessage.Metadata[tracing.MetadataTraceID], outboxMessage.Metadata[tracing.MetadataSpanID])
		_, span, finishSpan := tracing.StartSpan(ctx, tracerName, "message_received", trace.WithSpanKind(trace.SpanKindConsumer))

		pretty, err := prettyJson(outboxMessage.Payload)
		if err != nil {
			slog.Error(
				"prettyJson",
				"messageId", deref(sqsMessage.MessageId),
				"error", err,
			)
			finishSpan(err)
			continue
		}

		// slog would escape the json string, so we use log.Printf
		log.Printf("Message received:\n%s", pretty)

		span.SetAttributes(attribute.String("pretty", pretty))

		finishSpan(nil)
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

func messageCtx(ctx context.Context, message outbox.Message) context.Context {
	traceID, err := trace.TraceIDFromHex(message.Metadata[tracing.MetadataTraceID])
	if err != nil {
		return ctx
	}

	parentSpanID, err := trace.SpanIDFromHex(message.Metadata[tracing.MetadataSpanID])
	if err != nil {
		return ctx
	}

	// Create a span context that links back to the writer trace/span
	spanContext := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID,
		SpanID:  parentSpanID,
		Remote:  true,
	})

	return trace.ContextWithRemoteSpanContext(ctx, spanContext)
}
