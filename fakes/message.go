package fakes

import (
	"encoding/json"
	"github.com/brianvoe/gofakeit"
	"outbox"
)

type payload struct {
	Content string `json:"content"`
}

// TODO: polish
func FakeMessage() outbox.Message {
	p := payload{Content: gofakeit.Quote()}

	var metadata map[string]interface{}

	if gofakeit.Bool() {
		metadata = map[string]interface{}{
			"key": gofakeit.Word(),
		}
	}

	pp, _ := json.Marshal(p)

	return outbox.Message{
		Broker:   gofakeit.Word(),
		Topic:    gofakeit.Word(),
		Metadata: metadata,
		Payload:  pp,
	}
}
