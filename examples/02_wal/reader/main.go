package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/nikolayk812/pgx-outbox/wal"
)

const (
	// Postgres
	connStr     = "postgres://user:password@localhost:5432/dbname?replication=database"
	outboxTable = "outbox_messages"
	publication = "publication"
	slot        = "slot"
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

	ctx := context.Background()

	reader, err := wal.NewReader(connStr, outboxTable, publication, slot)
	if err != nil {
		gErr = fmt.Errorf("wal.NewReader: %w", err)
		return
	}
	defer func() {
		reader.Close()
	}()

	messageCh, errorCh, err := reader.Start(ctx)
	if err != nil {
		gErr = fmt.Errorf("reader.Start: %w", err)
		return
	}

	for rawMessage := range messageCh {
		slog.Info("Received raw message", "message", rawMessage)
	}

	for err := range errorCh {
		slog.Error("Error from reader", "error", err)
	}
}
