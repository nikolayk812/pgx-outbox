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

	var metadata map[string]interface{}

	if gofakeit.Bool() {
		metadata = map[string]interface{}{
			"string": gofakeit.Word(),
			"int":    gofakeit.Int64(),
			"bool":   gofakeit.Bool(),
		}
	}

	p := payload{Content: gofakeit.Quote()}
	pp, _ := json.Marshal(p)

	return outbox.Message{
		Broker:   gofakeit.Word(),
		Topic:    gofakeit.Word(),
		Metadata: metadata,
		Payload:  pp,
	}
}
