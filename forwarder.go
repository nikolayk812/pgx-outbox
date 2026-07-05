package outbox

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nikolayk812/pgx-outbox/types"
)

// Forwarder reads unpublished messages from the outbox table, publishes them and then marks them as published.
// It is recommended to run a single Forwarder instance per outbox table, i.e. in Kubernetes cronjob,
// or at least to isolate different Forwarder instances acting on the same outbox table by using different filters in outbox.Reader.
type Forwarder interface {
	Forward(ctx context.Context, limit int) (types.ForwardOutput, error)
}

type forwarder struct {
	reader    Reader
	publisher Publisher
	filter    types.MessageFilter
	pool      *pgxpool.Pool
}

func NewForwarder(reader Reader, publisher Publisher, opts ...ForwardOption) (Forwarder, error) {
	if reader == nil {
		return nil, ErrReaderNil
	}
	if publisher == nil {
		return nil, ErrPublisherNil
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

	fwd, err := NewForwarder(reader, publisher, opts...)
	if err != nil {
		return nil, fmt.Errorf("NewForwarder: %w", err)
	}

	f, ok := fwd.(*forwarder)
	if !ok {
		return nil, fmt.Errorf("unexpected forwarder type %T", fwd)
	}
	f.pool = pool

	return f, nil
}

// Forward reads unpublished messages from the outbox table according to the limit and filter in outbox.Reader,
// publishes them and then marks them as published in the outbox table.
// It returns an output with messages read, published and acknowledged.
// If any of the messages fail to be published, the function returns an error immediately.
// It means that the messages published before the error occurred will be not be acknowledged
// and will be published again on the next run.
// If a message cannot be published for any reason, it would block the forwarder from making progress.
// Hence, the forwarder progress (running in a cronjob) should be monitored and
// an action should be taken if it stops making progress, i.e. removing a poison message from the outbox table manually.
func (f *forwarder) Forward(ctx context.Context, limit int) (types.ForwardOutput, error) {
	var fs types.ForwardOutput

	r, ok := f.reader.(*reader)
	if !ok || f.pool == nil {
		messages, err := f.reader.Read(ctx, limit)
		if err != nil {
			return fs, fmt.Errorf("reader.Read: %w", err)
		}

		if len(messages) == 0 {
			return fs, nil
		}

		fs.Read = messages

		for idx, message := range messages {
			if err := f.publisher.Publish(ctx, message); err != nil {
				return fs, fmt.Errorf("publisher.Publish index[%d] topic[%s] id[%d]: %w", idx, message.Topic, message.ID, err)
			}
			fs.PublishedIDs = append(fs.PublishedIDs, message.ID)
		}

		ids := types.Messages(messages).IDs()

		fs.AckedIDs, err = f.reader.Ack(ctx, ids)
		if err != nil {
			return fs, fmt.Errorf("reader.Ack count[%d]: %w", len(ids), err)
		}

		return fs, nil
	}

	tx, err := f.pool.Begin(ctx)
	if err != nil {
		return fs, fmt.Errorf("pool.Begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	messages, err := r.readTx(ctx, tx, limit)
	if err != nil {
		return fs, fmt.Errorf("reader.readTx: %w", err)
	}

	if len(messages) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return fs, fmt.Errorf("tx.Commit empty: %w", err)
		}
		return fs, nil
	}

	fs.Read = messages

	for idx, message := range messages {
		if err := f.publisher.Publish(ctx, message); err != nil {
			return fs, fmt.Errorf("publisher.Publish index[%d] topic[%s] id[%d]: %w", idx, message.Topic, message.ID, err)
		}
		fs.PublishedIDs = append(fs.PublishedIDs, message.ID)
	}

	ids := types.Messages(messages).IDs()

	fs.AckedIDs, err = r.ackTx(ctx, tx, ids)
	if err != nil {
		return fs, fmt.Errorf("reader.ackTx count[%d]: %w", len(ids), err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fs, fmt.Errorf("tx.Commit: %w", err)
	}

	return fs, nil
}
