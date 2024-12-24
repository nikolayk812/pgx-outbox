package sns

import (
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"outbox/types"
)

type Transformer interface {
	Transform(message types.Message) (*sns.PublishInput, error)
}
