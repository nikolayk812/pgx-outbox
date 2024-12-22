package outbox

import "context"

type Publisher interface {
	Publish(ctx context.Context, message Message) error
}
