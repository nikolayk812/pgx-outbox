package sns

import (
	"context"
	"fmt"

	awsSns "github.com/aws/aws-sdk-go-v2/service/sns"
	outbox "github.com/nikolayk812/pgx-outbox"
	"github.com/nikolayk812/pgx-outbox/types"
)

type Publisher struct {
	snsClient   *awsSns.Client
	transformer MessageTransformer
}

func NewPublisher(snsClient *awsSns.Client, transformer MessageTransformer) (outbox.Publisher, error) {
	if snsClient == nil {
		return nil, ErrSnsClientNil
	}
	if transformer == nil {
		return nil, ErrTransformerNil
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

	input, err := p.transformer.Transform(ctx, message)
	if err != nil {
		return fmt.Errorf("transformer.Transform: %w", err)
	}

	if _, err := p.snsClient.Publish(ctx, input); err != nil {
		return fmt.Errorf("snsClient.Publish: %w", err)
	}

	return nil
}
