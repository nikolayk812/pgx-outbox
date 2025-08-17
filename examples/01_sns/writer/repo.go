package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	outbox "github.com/nikolayk812/pgx-outbox"
	"github.com/nikolayk812/pgx-outbox/examples/01_sns/clients/tracing"
	"github.com/nikolayk812/pgx-outbox/types"
	"go.opentelemetry.io/otel/attribute"
)

type Repo interface {
	CreateOrder(ctx context.Context, order Order) (Order, error)
}

type repo struct {
	pool *pgxpool.Pool

	writer outbox.Writer
	mapper OrderMessageMapper
}

func NewRepo(pool *pgxpool.Pool, writer outbox.Writer, mapper OrderMessageMapper) (Repo, error) {
	if pool == nil {
		return nil, fmt.Errorf("pool is nil")
	}
	if writer == nil {
		return nil, fmt.Errorf("writer is nil")
	}
	if mapper == nil {
		return nil, fmt.Errorf("mapper is nil")
	}

	return &repo{
		pool:   pool,
		writer: writer,
		mapper: mapper,
	}, nil
}

func (r *repo) CreateOrder(ctx context.Context, order Order) (o Order, txErr error) {
	ctx, span, finishSpan := tracing.StartSpan(ctx, tracerName, "order_created")

	tx, commitFunc, err := r.beginTx(ctx)
	if err != nil {
		return o, fmt.Errorf("beginTx: %w", err)
	}
	defer func() {
		if txErr = commitFunc(txErr); txErr != nil {
			txErr = fmt.Errorf("commitFunc: %w", txErr)
		}
		finishSpan(txErr)
	}()

	order, err = r.createOrder(ctx, tx, order)
	if err != nil {
		return o, fmt.Errorf("createOrder: %w", err)
	}

	message, err := r.mapper(order)
	if err != nil {
		return o, fmt.Errorf("mapper: %w", err)
	}
	message.Metadata = map[string]string{
		tracing.MetadataTraceID: span.SpanContext().TraceID().String(),
		tracing.MetadataSpanID:  span.SpanContext().SpanID().String(),
	}

	if _, err := r.writer.Write(ctx, tx, message); err != nil {
		return o, fmt.Errorf("writer.Write: %w", err)
	}

	span.SetAttributes(attribute.String("order.id", order.ID.String()))

	return order, nil
}

func (r *repo) createOrder(ctx context.Context, tx pgx.Tx, order Order) (o Order, _ error) {
	if tx == nil {
		return o, fmt.Errorf("tx is nil")
	}

	var createdAt time.Time
	err := tx.QueryRow(ctx,
		"INSERT INTO orders (id, customer_name, items_count) VALUES ($1, $2, $3) RETURNING created_at",
		order.ID, order.CustomerName, order.ItemsCount).
		Scan(&createdAt)
	if err != nil {
		return o, fmt.Errorf("tx.QueryRow: %w", err)
	}

	order.CreatedAt = createdAt
	return order, nil
}

type OrderMessageMapper types.ToMessageFunc[Order]
