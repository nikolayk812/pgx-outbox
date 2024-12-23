package outbox

import "context"

//go:generate mockery --name=Publisher --output=mocks --outpkg=mocks --filename=publisher_mock.go
type Publisher interface {
	Publish(ctx context.Context, message Message) error
}
