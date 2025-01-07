package sns

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsSns "github.com/aws/aws-sdk-go-v2/service/sns"
	awsTypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	outbox "github.com/nikolayk812/pgx-outbox"
	"github.com/nikolayk812/pgx-outbox/types"
	"go.opentelemetry.io/otel/trace"
)

type Publisher struct {
	snsClient   *awsSns.Client
	transformer MessageTransformer
}

func NewPublisher(snsClient *awsSns.Client, transformer MessageTransformer) (outbox.Publisher, error) {
	if snsClient == nil {
		return nil, fmt.Errorf("snsClient is nil")
	}
	if transformer == nil {
		return nil, fmt.Errorf("transformer is nil")
	}

	return &Publisher{
		snsClient:   snsClient,
		transformer: transformer,
	}, nil
}

func (p Publisher) Publish(ctx context.Context, message types.Message) error {
	if err := message.Validate(); err != nil {
		return fmt.Errorf("message.Validate: %w", err)
	}

	input, err := p.transformer.Transform(message)
	if err != nil {
		return fmt.Errorf("transformer.Transform: %w", err)
	}
	injectTraceID(ctx, message.Metadata[types.MetadataTraceID], input)

	if _, err := p.snsClient.Publish(ctx, input); err != nil {
		return fmt.Errorf("snsClient.Publish: %w", err)
	}

	return nil
}

func injectTraceID(ctx context.Context, traceID string, input *awsSns.PublishInput) {
	// Use the passed traceID if it is not empty, otherwise try to get from the context
	if traceID == "" {
		// Extract the traceID from the context if it's available
		span := trace.SpanFromContext(ctx)
		traceID = span.SpanContext().TraceID().String()
	}

	if traceID == "" {
		return
	}

	if input.MessageAttributes == nil {
		input.MessageAttributes = make(map[string]awsTypes.MessageAttributeValue)
	}

	input.MessageAttributes[types.MetadataTraceID] = awsTypes.MessageAttributeValue{
		DataType:    aws.String("String"),
		StringValue: aws.String(traceID),
	}
}
