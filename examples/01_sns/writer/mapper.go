package main

import (
	"encoding/json"
	"fmt"

	"github.com/brianvoe/gofakeit"
	outbox "github.com/nikolayk812/pgx-outbox/types"
)

const EventTypeOrderCreated = "order_created"

func orderToMessage(order Order) (m outbox.Message, _ error) {
	payload := OrderMessagePayload{
		ID:           order.ID,
		CustomerName: order.CustomerName,
		ItemsCount:   order.ItemsCount,
		CreatedAt:    order.CreatedAt,
		Quote:        gofakeit.Quote(),
		EventType:    EventTypeOrderCreated,
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
