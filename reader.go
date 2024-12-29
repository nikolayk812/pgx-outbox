package outbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nikolayk812/pgx-outbox/types"
)

//go:generate mockery --name=Reader --output=internal/mocks --outpkg=mocks --filename=reader_mock.go

// Reader reads outbox unpublished messages from a single outbox table.
// Users should prefer to interact directly with Forwarder instance instead of Reader.
// Read and Ack happen in different transactions.
type Reader interface {
	// Read reads unpublished messages from the outbox table that match the filter.
	// limit is the maximum number of messages to read.
	// Limit and frequency of Read invocations should be considered carefully to avoid overloading the database.
	Read(ctx context.Context, limit int) ([]types.Message, error)

	// Ack acknowledges / marks the messages by ids as published in a single transaction.
	// ids can be obtained from the Read method output.
	// It returns the number of messages acknowledged.
	Ack(ctx context.Context, ids []int64) (int, error)
}

type reader struct {
	pool   *pgxpool.Pool
	table  string
	filter types.MessageFilter
}

func NewReader(table string, pool *pgxpool.Pool, opts ...ReadOption) (Reader, error) {
	if pool == nil {
		return nil, errors.New("pool is nil")
	}
	if table == "" {
		return nil, ErrTableEmpty
	}

	r := &reader{
		pool:  pool,
		table: table,
	}

	if err := r.filter.Validate(); err != nil {
		return nil, fmt.Errorf("filter.Validate: %w", err)
	}

	for _, opt := range opts {
		opt(r)
	}

	return r, nil
}

// Read returns unpublished messages sorted by ID in ascending order.
// returns an error if
// - limit is LTE 0
// - SQL query building or DB call fails.
func (r *reader) Read(ctx context.Context, limit int) ([]types.Message, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be GT 0, got %d", limit)
	}

	sb := sq.StatementBuilder.PlaceholderFormat(sq.Dollar).
		Select("id", "broker", "topic", "metadata", "payload").
		From(r.table).
		Where(sq.Eq{"published_at": nil})

	sb = whereFilter(sb, r.filter)

	sb = sb.OrderBy("id ASC").Limit(uint64(limit))

	q, args, err := sb.ToSql()
	if err != nil {
		return nil, fmt.Errorf("sb.ToSql: %w", err)
	}

	rows, err := r.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("pool.Query: %w", err)
	}

	result, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (types.Message, error) {
		var msg types.Message
		if err := row.Scan(&msg.ID, &msg.Broker, &msg.Topic, &msg.Metadata, &msg.Payload); err != nil {
			return types.Message{}, fmt.Errorf("row.Scan: %w", err)
		}
		return msg, nil
	})
	if err != nil {
		return nil, fmt.Errorf("pgx.CollectRows: %w", err)
	}

	return result, nil
}

// Ack marks the messages by ids as published in a single transaction.
// It sets the published_at column to the current time, same for all ids.
// Non-existent and duplicate ids are skipped.
// returns an error if
// - SQL query building or DB call fails.
func (r *reader) Ack(ctx context.Context, ids []int64) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}

	now := time.Now().UTC()

	ub := sq.StatementBuilder.PlaceholderFormat(sq.Dollar).
		Update(r.table).
		Set("published_at", now).
		Where(sq.Eq{"id": ids}).
		Where(sq.Eq{"published_at": nil})

	q, args, err := ub.ToSql()
	if err != nil {
		return 0, fmt.Errorf("ub.ToSql: %w", err)
	}

	commandTag, err := r.pool.Exec(ctx, q, args...)
	if err != nil {
		return 0, fmt.Errorf("pool.Exec: %w", err)
	}

	return int(commandTag.RowsAffected()), nil
}

func whereFilter(sb sq.SelectBuilder, filter types.MessageFilter) sq.SelectBuilder {
	if len(filter.Brokers) > 0 {
		sb = sb.Where(sq.Eq{"broker": filter.Brokers})
	}

	if len(filter.Topics) > 0 {
		sb = sb.Where(sq.Eq{"topic": filter.Topics})
	}

	return sb
}
