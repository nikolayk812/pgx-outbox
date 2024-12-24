package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/brianvoe/gofakeit"
	"github.com/google/uuid"

	"github.com/jackc/pgx/v5/pgxpool"
	outbox "github.com/nikolayk812/pgx-outbox"
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

	writer, err := outbox.NewWriter("outbox_messages")
	if err != nil {
		gErr = fmt.Errorf("outbox.NewWriter: %w", err)
		return
	}

	ctx := context.Background()

	connStr := "postgres://user:password@localhost:5432/dbname"

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		gErr = fmt.Errorf("pgxpool.New: %w", err)
		return
	}

	r := &repo{
		pool:   pool,
		writer: writer,
	}

	for {
		user := User{
			ID:   uuid.New(),
			Name: gofakeit.Name(),
			Age:  gofakeit.Number(18, 100),
		}

		err := r.CreateUser(ctx, user)
		if err != nil {
			gErr = fmt.Errorf("r.CreateUser: %w", err)
			return
		}
	}

}
