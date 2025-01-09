package sns

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/nikolayk812/pgx-outbox/types"
)

type MessageTransformer interface {
	Transform(ctx context.Context, message types.Message) (*sns.PublishInput, error)
}
