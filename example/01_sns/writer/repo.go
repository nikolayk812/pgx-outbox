package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	outbox "github.com/nikolayk812/pgx-outbox"
	"github.com/nikolayk812/pgx-outbox/fakes"
)

type repo struct {
	pool   *pgxpool.Pool
	writer outbox.Writer
}

func (r *repo) CreateUser(ctx context.Context, user User) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pool.Begin: %w", err)
	}

	user, err = r.createUser(ctx, tx, user)
	if err != nil {
		return fmt.Errorf("createUser: %w", err)
	}

	// TODO: convert user to message
	message := fakes.FakeMessage()

	if _, err := r.writer.Write(ctx, tx, message); err != nil {
		return fmt.Errorf("writer.Write: %w", err)
	}

	return commit(ctx, tx)
}

func (r *repo) createUser(ctx context.Context, tx pgx.Tx, user User) (u User, _ error) {
	if tx == nil {
		return u, fmt.Errorf("tx is nil")
	}

	var createdAt time.Time
	err := tx.QueryRow(ctx,
		"INSERT INTO users (id, name, age, created_at) VALUES ($1, $2, $3, $4) RETURNING created_at",
		user.ID, user.Name, user.Age, user.CreatedAt).
		Scan(&createdAt)
	if err != nil {
		return u, fmt.Errorf("tx.QueryRow: %w", err)
	}

	u.CreatedAt = createdAt
	return u, nil
}

func commit(ctx context.Context, tx pgx.Tx) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}

	if err := tx.Commit(ctx); err != nil {
		if rbErr := tx.Rollback(ctx); rbErr != nil {
			return fmt.Errorf("tx.Rollback %v: %w", rbErr, err) //nolint:errorlint
		}
		return fmt.Errorf("tx.Commit: %w", err)
	}

	return nil
}
