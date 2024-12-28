package outbox

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nikolayk812/pgx-outbox/types"
)

// Forwarder reads unpublished messages from the outbox table, publishes them and then marks them as published.
// It is recommended to run a single Forwarder instance per outbox table, i.e. in Kubernetes cronjob,
// or at least to isolate Forwarder instances acting on the same outbox table by using different filters.
type Forwarder interface {
	Forward(ctx context.Context, limit int) (types.ForwardStats, error)
}

type forwarder struct {
	reader    Reader
	publisher Publisher
	filter    types.MessageFilter
}

func NewForwarder(reader Reader, publisher Publisher, opts ...ForwardOption) (Forwarder, error) {
	if reader == nil {
		return nil, errors.New("reader is nil")
	}
	if publisher == nil {
		return nil, errors.New("publisher is nil")
	}

	f := &forwarder{
		reader:    reader,
		publisher: publisher,
	}

	for _, opt := range opts {
		opt(f)
	}

	if err := f.filter.Validate(); err != nil {
		return nil, fmt.Errorf("filter.Validate: %w", err)
	}

	return f, nil
}

func NewForwarderFromPool(table string, pool *pgxpool.Pool, publisher Publisher, opts ...ForwardOption) (Forwarder, error) {
	reader, err := NewReader(table, pool)
	if err != nil {
		return nil, fmt.Errorf("NewReader: %w", err)
	}

	forwarder, err := NewForwarder(reader, publisher, opts...)
	if err != nil {
		return nil, fmt.Errorf("NewForwarder: %w", err)
	}

	return forwarder, nil
}

// TODO: comment.
func (f *forwarder) Forward(ctx context.Context, limit int) (types.ForwardStats, error) {
	var fs types.ForwardStats

	messages, err := f.reader.Read(ctx, limit)
	if err != nil {
		return fs, fmt.Errorf("reader.Read: %w", err)
	}

	if len(messages) == 0 {
		return fs, nil
	}

	fs.Read = len(messages)

	for idx, message := range messages {
		if err := f.publisher.Publish(ctx, message); err != nil {
			return fs, fmt.Errorf("publisher.Publish index[%d] topic[%s] id[%d]: %w", idx, message.Topic, message.ID, err)
		}
		fs.Published++
	}

	ids := types.Messages(messages).IDs()

	// if it fails here, messages would be published again on the next run
	marked, err := f.reader.Ack(ctx, ids)
	if err != nil {
		return fs, fmt.Errorf("reader.Ack count[%d]: %w", len(ids), err)
	}

	fs.Acked = marked

	return fs, nil
}
