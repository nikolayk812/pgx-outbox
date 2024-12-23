package outbox

import (
	"context"
	"errors"
	"fmt"
	"github.com/samber/lo"
)

type Forwarder interface {
	Publish(ctx context.Context, filter MessageFilter, limit int) error
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

// TODO: return statistics?
func (f *forwarder) Publish(ctx context.Context, filter MessageFilter, limit int) error {
	messages, err := f.reader.Read(ctx, filter, limit)
	if err != nil {
		return fmt.Errorf("reader.Read: %w", err)
	}

	for idx, message := range messages {
		if err := f.publisher.Publish(ctx, message); err != nil {
			return fmt.Errorf("publisher.Publish index[%d] id[%d]: %w", idx, message.ID, err)
		}
	}

	ids := lo.Map(messages, func(m Message, _ int) int64 {
		return m.ID
	})

	// if it fails here, messages would be published again on the next run
	_, err = f.reader.Mark(ctx, ids)
	if err != nil {
		return fmt.Errorf("reader.Mark count[%d]: %w", len(ids), err)
	}

	return nil
}
