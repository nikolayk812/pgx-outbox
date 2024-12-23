package outbox

import (
	"fmt"
)

type Message struct {
	ID        int64
	EventType string `validate:"required"`
	Broker    string `validate:"required"`
	Topic     string `validate:"required"`
	Payload   []byte `validate:"required"`
}

func (m *Message) Validate() error {
	v, err := getValidator()
	if err != nil {
		return fmt.Errorf("getValidator: %w", err)
	}

	return v.Struct(m)
}
