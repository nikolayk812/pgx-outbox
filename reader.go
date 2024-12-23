package outbox

import (
	"context"
	"errors"
	"fmt"
	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TODO: comments
type Reader interface {
	Read(ctx context.Context, filter MessageFilter, limit int) ([]Message, error)
	Mark(ctx context.Context, ids []int64) (int64, error) // TODO: rename to Ack?

	// TODO: add Delete?
}

type reader struct {
	pool  *pgxpool.Pool
	table string
}

func NewReader(pool *pgxpool.Pool, table string) (Reader, error) {
	if pool == nil {
		return nil, errors.New("pool is nil")
	}
	if table == "" {
		return nil, errors.New("table is empty")
	}

	return &reader{
		pool:  pool,
		table: table,
	}, nil
}

func (r *reader) Read(ctx context.Context, filter MessageFilter, limit int) ([]Message, error) {
	if err := filter.Validate(); err != nil {
		return nil, fmt.Errorf("filter.Validate: %w", err)
	}

	if limit <= 0 {
		return nil, fmt.Errorf("limit must be GT 0, got %d", limit)
	}

	sb := sq.StatementBuilder.PlaceholderFormat(sq.Dollar).
		Select("id", "broker", "topic", "metadata", "payload").
		From(r.table).
		Where(sq.Eq{"published_at": nil})

	sb = whereFilter(sb, filter)

	sb = sb.OrderBy("id ASC").Limit(uint64(limit))

	q, args, err := sb.ToSql()
	if err != nil {
		return nil, fmt.Errorf("sb.ToSql: %w", err)
	}

	rows, err := r.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("pool.Query: %w", err)
	}

	result, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Message, error) {
		var msg Message
		if err := row.Scan(&msg.ID, &msg.Broker, &msg.Topic, &msg.Metadata, &msg.Payload); err != nil {
			return Message{}, fmt.Errorf("row.Scan: %w", err)
		}
		return msg, nil
	})
	if err != nil {
		return nil, fmt.Errorf("pgx.CollectRows: %w", err)
	}

	return result, nil
}

func (r *reader) Mark(ctx context.Context, ids []int64) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}

	ub := sq.StatementBuilder.PlaceholderFormat(sq.Dollar).
		Update(r.table).
		Set("published_at", sq.Expr("NOW()")).
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

	return commandTag.RowsAffected(), nil
}

func whereFilter(sb sq.SelectBuilder, filter MessageFilter) sq.SelectBuilder {
	if len(filter.Brokers) > 0 {
		sb = sb.Where(sq.Eq{"broker": filter.Brokers})
	}

	if len(filter.Topics) > 0 {
		sb = sb.Where(sq.Eq{"topic": filter.Topics})
	}

	return sb
}
