package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/brianvoe/gofakeit"
	"github.com/google/uuid"

	"github.com/jackc/pgx/v5/pgxpool"
	outbox "github.com/nikolayk812/pgx-outbox"
)

const (
	connStr     = "postgres://user:password@localhost:5432/dbname"
	outboxTable = "outbox_messages"
	topic       = "topic1"
)

func main() {
	var gErr error

	defer func() {
		if gErr != nil {
			slog.Error("global error", "error", gErr)
			os.Exit(1)
		}

		os.Exit(0)
	}()

	writer, err := outbox.NewWriter(outboxTable)
	if err != nil {
		gErr = fmt.Errorf("outbox.NewWriter: %w", err)
		return
	}

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		gErr = fmt.Errorf("pgxpool.New: %w", err)
		return
	}

	repo, err := NewRepo(pool, writer, userToMessage)
	if err != nil {
		gErr = fmt.Errorf("NewRepo: %w", err)
		return
	}

	for {
		user := User{
			ID:   uuid.New(),
			Name: gofakeit.Name(),
			Age:  gofakeit.Number(18, 100),
		}

		user, err = repo.CreateUser(ctx, user)
		if err != nil {
			gErr = fmt.Errorf("r.CreateUser: %w", err)
			return
		}

		slog.Info("user created", "user", user)

		time.Sleep(1500 * time.Millisecond)
	}
}
