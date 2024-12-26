package sns

import (
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/nikolayk812/pgx-outbox/types"
)

type MessageTransformer interface {
	Transform(message types.Message) (*sns.PublishInput, error)
}
