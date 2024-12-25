package main

import (
	"log/slog"
	"os"
)

const (
	region   = "eu-central-1"
	endpoint = "http://localhost:4566"
	topic    = "topic1"
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

	// ctx := context.Background()

}
