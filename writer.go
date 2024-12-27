package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/nikolayk812/pgx-outbox/types"
)

// Writer writes outbox messages to a single outbox table.
// To write messages to multiple outbox tables, create multiple Writer instances.
// An outbox message must be written in the same transaction as the business data,
// hence the tx argument which supports both pgx.Tx and *sql.Tx.
// Implementations must be safe for concurrent use by multiple goroutines.
type Writer interface {
	// Write writes the message to the outbox table.
	// It returns the ID of the newly inserted message.
	Write(ctx context.Context, tx types.Tx, message types.Message) (int64, error)
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
// - tx is nil or unsupported
// - message is invalid
// - write operation fails.
func (w *writer) Write(ctx context.Context, tx types.Tx, message types.Message) (int64, error) {
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

	query, args, err := ib.ToSql()
	if err != nil {
		return 0, fmt.Errorf("ib.ToSql: %w", err)
	}

	row, err := queryRow(ctx, tx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("queryRow: %w", err)
	}

	var id int64
	if err := row.Scan(&id); err != nil {
		return 0, fmt.Errorf("row.Scan: %w", err)
	}

	return id, nil
}

func queryRow(ctx context.Context, tx types.Tx, q string, args ...interface{}) (pgx.Row, error) {
	if tx == nil {
		return nil, errors.New("tx is nil")
	}

	var row pgx.Row

	switch t := tx.(type) {
	case *sql.Tx:
		row = t.QueryRowContext(ctx, q, args...)
	case pgx.Tx:
		row = t.QueryRow(ctx, q, args...)
	default:
		return nil, errors.New("unsupported transaction type")
	}

	return row, nil
}
