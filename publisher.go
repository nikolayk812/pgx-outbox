package outbox

import (
	"context"

	"github.com/nikolayk812/pgx-outbox/types"
)

//go:generate mockery --name=Publisher --output=mocks --outpkg=mocks --filename=publisher_mock.go
type Publisher interface {
	Publish(ctx context.Context, message types.Message) error
}
