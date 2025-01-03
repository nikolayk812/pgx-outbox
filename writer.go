package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/nikolayk812/pgx-outbox/types"
)

// Writer writes outbox messages to a single outbox table.
// To write messages to multiple outbox tables, create multiple Writer instances.
// An outbox message must be written in the same transaction as business entities,
// hence the tx argument which supports both pgx.Tx and *sql.Tx.
// Implementations must be safe for concurrent use by multiple goroutines.
type Writer interface {
	// Write writes the message to the outbox table.
	// It returns the ID of the newly inserted message.
	Write(ctx context.Context, tx Tx, message types.Message) (int64, error)

	// WriteBatch writes multiple messages to the outbox table.
	// It returns the IDs of the newly inserted messages.
	// It returns an error if any of the messages fail to write.
	WriteBatch(ctx context.Context, tx pgx.Tx, messages []types.Message) ([]int64, error)
}

type writer struct {
	table            string
	usePreparedBatch bool
}

func NewWriter(table string, opts ...WriteOption) (Writer, error) {
	if table == "" {
		return nil, ErrTableEmpty
	}

	w := &writer{
		table:            table,
		usePreparedBatch: true,
	}

	for _, opt := range opts {
		opt(w)
	}

	return w, nil
}

// Write returns an error if
// - tx is nil or unsupported
// - message is invalid
// - write operation fails.
func (w *writer) Write(ctx context.Context, tx Tx, message types.Message) (int64, error) {
	if tx == nil {
		return 0, ErrTxNil
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

// WriteBatch uses batching feature of the pgx driver,
// by default it uses prepared statements for batch writes, but it can be disabled using WithDisablePreparedBatch option.
// WriteBatch returns an error if
// - tx is nil
// - any message is invalid
// - write operation fails.
//
//nolint:cyclop
func (w *writer) WriteBatch(ctx context.Context, tx pgx.Tx, messages []types.Message) (_ []int64, txErr error) {
	if tx == nil {
		return nil, ErrTxNil
	}

	if len(messages) == 0 {
		return nil, nil
	}

	if err := types.Messages(messages).Validate(); err != nil {
		return nil, fmt.Errorf("messages.Validate: %w", err)
	}

	if len(messages) == 1 {
		id, err := w.Write(ctx, tx, messages[0])
		if err != nil {
			return nil, fmt.Errorf("w.Write: %w", err)
		}
		return []int64{id}, nil
	}

	query := fmt.Sprintf("INSERT INTO %s (broker, topic, metadata, payload) VALUES ($1, $2, $3, $4) RETURNING id", w.table)

	if w.usePreparedBatch {
		prepareStatementName := fmt.Sprintf("%s_write_batch_%d", w.table, time.Now().UnixMilli())

		_, err := tx.Prepare(ctx, prepareStatementName, query)
		if err != nil {
			return nil, fmt.Errorf("tx.Prepare: %w", err)
		}

		query = prepareStatementName
	}

	batch := &pgx.Batch{}
	for _, message := range messages {
		batch.Queue(query,
			message.Broker, message.Topic, message.Metadata, string(message.Payload))
	}

	br := tx.SendBatch(ctx, batch)
	defer func() {
		if err := br.Close(); err != nil {
			txErr = fmt.Errorf("br.Close: %w", err)
		}
	}()

	// Collect all returned IDs
	ids := make([]int64, 0, len(messages))
	for range messages {
		row := br.QueryRow()
		var id int64
		if err := row.Scan(&id); err != nil {
			return nil, fmt.Errorf("row.Scan: %w", err)
		}
		ids = append(ids, id)
	}

	return ids, nil
}

// Tx is a transaction interface to support both and pgx.Tx and *sql.Tx.
// As pgx.Tx and *sql.Tx do not have common method signatures, this is empty interface.
type Tx interface{}

func queryRow(ctx context.Context, tx Tx, q string, args ...interface{}) (pgx.Row, error) {
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
		return nil, fmt.Errorf("%w: %T", ErrTxUnsupportedType, tx)
	}

	return row, nil
}
