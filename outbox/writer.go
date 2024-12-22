package outbox

import (
	"context"
	"errors"
	"fmt"
	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
)

type Writer interface {
	Write(ctx context.Context, tx pgx.Tx, message Message) error

	// TODO: add WriteBatch?
}

type writer struct {
	table string
}

func NewWriter(table string) (Writer, error) {
	if table == "" {
		return nil, errors.New("table is empty")
	}

	return &writer{table: table}, nil
}

func (w *writer) Write(ctx context.Context, tx pgx.Tx, message Message) error {
	if tx == nil {
		return errors.New("tx is nil")
	}

	if err := message.Validate(); err != nil {
		return fmt.Errorf("message.Validate: %w", err)
	}

	ib := sq.StatementBuilder.PlaceholderFormat(sq.Dollar).
		Insert(w.table).
		Columns("event_type", "broker", "topic", "payload")

	ib = ib.Values(message.EventType, message.Broker, message.Topic, message.Payload)

	q, args, err := ib.ToSql()
	if err != nil {
		return fmt.Errorf("ib.ToSql: %w", err)
	}

	if _, err := tx.Exec(ctx, q, args...); err != nil {
		return fmt.Errorf("tx.Exec: %w", err)
	}

	return nil
}
