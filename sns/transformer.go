package sns

import (
	"github.com/nikolayk812/pgx-outbox/types"

	"github.com/aws/aws-sdk-go-v2/service/sns"
)

type MessageTransformer interface {
	Transform(message types.Message) (*sns.PublishInput, error)
}
