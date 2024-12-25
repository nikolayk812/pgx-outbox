package main

import (
	"encoding/json"
	"fmt"

	"github.com/brianvoe/gofakeit"
	outbox "github.com/nikolayk812/pgx-outbox/types"
)

func userToMessage(user User) (m outbox.Message, _ error) {
	payload := UserMessagePayload{
		ID:        user.ID,
		Name:      user.Name,
		Age:       user.Age,
		CreatedAt: user.CreatedAt,
		Quote:     gofakeit.Quote(),
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return m, fmt.Errorf("json.Marshal: %w", err)
	}

	message := outbox.Message{
		Broker:  "sns",
		Topic:   "user_created",
		Payload: payloadBytes,
	}

	return message, nil
}
