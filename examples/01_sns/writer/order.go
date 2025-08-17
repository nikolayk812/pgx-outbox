package main

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Order struct {
	ID           uuid.UUID
	CustomerName string
	ItemsCount   int
	CreatedAt    time.Time
}

func (o Order) String() string {
	return fmt.Sprintf("[customer_name=%s, items_count=%d, id=%s, created=%s]", o.CustomerName, o.ItemsCount, o.ID, o.CreatedAt.Format(time.DateTime))
}

type OrderMessagePayload struct {
	ID           uuid.UUID `json:"id"`
	CustomerName string    `json:"customer_name"`
	ItemsCount   int       `json:"items_count"`
	CreatedAt    time.Time `json:"created_at"`

	Quote     string `json:"quote"`
	EventType string `json:"event_type"`
}
