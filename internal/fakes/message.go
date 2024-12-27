package fakes

import (
	"encoding/json"

	"github.com/brianvoe/gofakeit"
	"github.com/nikolayk812/pgx-outbox/types"
)

func FakeMessage() types.Message {
	var metadata map[string]string

	if gofakeit.Bool() {
		metadata = map[string]string{
			"word":  gofakeit.Word(),
			"quote": gofakeit.Quote(),
		}
	}

	p := payload{Content: gofakeit.Quote()}
	pp, _ := json.Marshal(p)

	return types.Message{
		Broker:   gofakeit.Word(),
		Topic:    gofakeit.Word(),
		Metadata: metadata,
		Payload:  pp,
	}
}

type payload struct {
	Content string `json:"content"`
}
