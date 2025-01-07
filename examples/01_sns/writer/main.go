package main

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/brianvoe/gofakeit"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	outbox "github.com/nikolayk812/pgx-outbox"
	"github.com/nikolayk812/pgx-outbox/examples/01_sns/clients/tracing"
	"github.com/spf13/viper"
)

const (
	defaultConnStr  = "postgres://user:password@localhost:5432/dbname"
	outboxTable     = "outbox_messages"
	topic           = "topic1"
	defaultInterval = 1500 * time.Millisecond

	defaultTracingEndpoint = "localhost:4317"
	tracerName             = "pgx-outbox-writer"
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

	viper.AutomaticEnv()

	tracingEndpoint := cmp.Or(viper.GetString("TRACING_ENDPOINT"), defaultTracingEndpoint)
	dbURL := cmp.Or(viper.GetString("DB_URL"), defaultConnStr)
	interval := cmp.Or(viper.GetDuration("WRITER_INTERVAL"), defaultInterval)

	ctx := context.Background()

	shutdownTracer, err := tracing.InitGrpcTracer(ctx, tracingEndpoint, tracerName)
	if err != nil {
		gErr = fmt.Errorf("tracing.InitGrpcTracer: %w", err)
		return
	}
	defer shutdownTracer()

	writer, err := outbox.NewWriter(outboxTable)
	if err != nil {
		gErr = fmt.Errorf("outbox.NewWriter: %w", err)
		return
	}

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		gErr = fmt.Errorf("pgxpool.New: %w", err)
		return
	}

	repo, err := NewRepo(pool, writer, userToMessage)
	if err != nil {
		gErr = fmt.Errorf("NewRepo: %w", err)
		return
	}

	slog.Info("Writer Ready") // integration test waits for this message

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

		time.Sleep(interval)
	}
}
