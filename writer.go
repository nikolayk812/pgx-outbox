package outbox

import (
	"context"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/nikolayk812/pgx-outbox/types"
)

// Writer writes outbox messages to a single outbox table.
// To write messages to multiple outbox tables, create multiple Writer instances.
// An outbox message must be written in the same transaction as the business data, hence the pgx.Tx argument.
// Implementations must be safe for concurrent use by multiple goroutines.
type Writer interface {
	// Write writes the message to the outbox table.
	// It returns the ID of the newly inserted message.
	Write(ctx context.Context, tx pgx.Tx, message types.Message) (int64, error)

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

// Write returns an error if
// - tx is nil
// - message is invalid
// - write operation fails.
func (w *writer) Write(ctx context.Context, tx pgx.Tx, message types.Message) (int64, error) {
	if tx == nil {
		return 0, errors.New("tx is nil")
	}

	if err := message.Validate(); err != nil {
		return 0, fmt.Errorf("message.Validate: %w", err)
	}

	ib := sq.StatementBuilder.PlaceholderFormat(sq.Dollar).
		Insert(w.table).
		Columns("broker", "topic", "metadata", "payload").
		Values(message.Broker, message.Topic, message.Metadata, string(message.Payload)).
		Suffix("RETURNING id")

	q, args, err := ib.ToSql()
	if err != nil {
		return 0, fmt.Errorf("ib.ToSql: %w", err)
	}

	var id int64
	if err := tx.QueryRow(ctx, q, args...).Scan(&id); err != nil {
		return 0, fmt.Errorf("tx.Exec: %w", err)
	}

	return id, nil
}
