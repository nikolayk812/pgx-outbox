package main

import (
	"encoding/json"
	"fmt"

	"github.com/brianvoe/gofakeit"
	outbox "github.com/nikolayk812/pgx-outbox/types"
)

const EventTypeUserCreated = "user_created"

func userToMessage(user User) (m outbox.Message, _ error) {
	payload := UserMessagePayload{
		ID:        user.ID,
		Name:      user.Name,
		Age:       user.Age,
		CreatedAt: user.CreatedAt,
		Quote:     gofakeit.Quote(),
		EventType: EventTypeUserCreated,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return m, fmt.Errorf("json.Marshal: %w", err)
	}

	message := outbox.Message{
		Broker:  "sns",
		Topic:   topic,
		Payload: payloadBytes,
	}

	return message, nil
}
