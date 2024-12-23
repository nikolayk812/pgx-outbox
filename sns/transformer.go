package sns

import (
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"outbox"
)

type Transformer interface {
	Transform(message outbox.Message) (*sns.PublishInput, error)
}
