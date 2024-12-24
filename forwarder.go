package outbox

import (
	"context"
	"errors"
	"fmt"

	"github.com/nikolayk812/pgx-outbox/types"
)

// Forwarder reads unpublished messages from the outbox table, publishes them and then marks them as published.
// It is recommended to run a single Forwarder instance per outbox table, i.e. in Kubernetes cronjob,
// or at least to isolate Forwarder instances acting on the same outbox table by using different filters.
type Forwarder interface {
	Forward(ctx context.Context, filter types.MessageFilter, limit int) (types.ForwardStats, error)
}

type forwarder struct {
	reader    Reader
	publisher Publisher
}

func NewForwarder(reader Reader, publisher Publisher) (Forwarder, error) {
	if reader == nil {
		return nil, errors.New("reader is nil")
	}
	if publisher == nil {
		return nil, errors.New("publisher is nil")
	}

	return &forwarder{
		reader:    reader,
		publisher: publisher,
	}, nil
}

// TODO: comment.
func (f *forwarder) Forward(ctx context.Context, filter types.MessageFilter, limit int) (fs types.ForwardStats, _ error) {
	messages, err := f.reader.Read(ctx, filter, limit)
	if err != nil {
		return fs, fmt.Errorf("reader.Read: %w", err)
	}

	if len(messages) == 0 {
		return fs, nil
	}

	fs.Read = len(messages)

	for idx, message := range messages {
		if err := f.publisher.Publish(ctx, message); err != nil {
			return fs, fmt.Errorf("publisher.Publish index[%d] id[%d]: %w", idx, message.ID, err)
		}
		fs.Published++
	}

	ids := types.Messages(messages).IDs()

	// if it fails here, messages would be published again on the next run
	marked, err := f.reader.Ack(ctx, ids)
	if err != nil {
		return fs, fmt.Errorf("reader.Ack count[%d]: %w", len(ids), err)
	}

	fs.Marked = marked

	return fs, nil
}
