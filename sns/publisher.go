package sns

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"outbox"
)

type Publisher struct {
	snsClient   *sns.Client
	transformer Transformer
}

func NewPublisher(snsClient *sns.Client, transformer Transformer) (outbox.Publisher, error) {
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

func (p Publisher) Publish(ctx context.Context, message outbox.Message) error {
	if err := message.Validate(); err != nil {
		return fmt.Errorf("message.Validate: %w", err)
	}

	input, err := p.transformer.Transform(message)
	if err != nil {
		return fmt.Errorf("transformer.Transform: %w", err)
	}

	if _, err := p.snsClient.Publish(ctx, input); err != nil {
		return fmt.Errorf("snsClient.Publish: %w", err)
	}

	return nil
}
