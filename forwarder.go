package outbox

import (
	"context"
	"errors"
	"fmt"
)

type Forwarder interface {
	Forward(ctx context.Context, filter MessageFilter, limit int) (ForwardingStats, error)
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

func (f *forwarder) Forward(ctx context.Context, filter MessageFilter, limit int) (fs ForwardingStats, _ error) {
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

	ids := Messages(messages).IDs()

	// if it fails here, messages would be published again on the next run
	marked, err := f.reader.Mark(ctx, ids)
	if err != nil {
		return fs, fmt.Errorf("reader.Mark count[%d]: %w", len(ids), err)
	}

	fs.Marked = int(marked)

	return fs, nil
}

type ForwardingStats struct {
	Read      int
	Published int
	Marked    int
}
