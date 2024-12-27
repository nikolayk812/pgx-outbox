package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	outbox "github.com/nikolayk812/pgx-outbox"
	"github.com/nikolayk812/pgx-outbox/types"
)

type Repo interface {
	CreateUser(ctx context.Context, user User) (User, error)
}

type repo struct {
	pool *pgxpool.Pool

	writer outbox.Writer
	mapper UserMessageMapper
}

func NewRepo(pool *pgxpool.Pool, writer outbox.Writer, mapper UserMessageMapper) (Repo, error) {
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

func (r *repo) CreateUser(ctx context.Context, user User) (u User, txErr error) {
	tx, commitFunc, err := r.beginTx(ctx)
	if err != nil {
		return u, fmt.Errorf("beginTx: %w", err)
	}
	defer func() {
		if txErr = commitFunc(txErr); txErr != nil {
			txErr = fmt.Errorf("commitFunc: %w", txErr)
		}
	}()

	user, err = r.createUser(ctx, tx, user)
	if err != nil {
		return u, fmt.Errorf("createUser: %w", err)
	}

	message, err := r.mapper(user)
	if err != nil {
		return u, fmt.Errorf("mapper: %w", err)
	}

	if _, err := r.writer.Write(ctx, tx, message); err != nil {
		return u, fmt.Errorf("writer.Write: %w", err)
	}

	return user, nil
}

func (r *repo) createUser(ctx context.Context, tx pgx.Tx, user User) (u User, _ error) {
	if tx == nil {
		return u, fmt.Errorf("tx is nil")
	}

	var createdAt time.Time
	err := tx.QueryRow(ctx,
		"INSERT INTO users (id, name, age) VALUES ($1, $2, $3) RETURNING created_at",
		user.ID, user.Name, user.Age).
		Scan(&createdAt)
	if err != nil {
		return u, fmt.Errorf("tx.QueryRow: %w", err)
	}

	user.CreatedAt = createdAt
	return user, nil
}

type UserMessageMapper types.ToMessageFunc[User]
