package outbox

import (
	"fmt"
)

type Message struct {
	ID       int64
	Broker   string `validate:"required"`
	Topic    string `validate:"required"`
	Metadata map[string]interface{}
	Payload  []byte `validate:"required,json"`
}

func (m *Message) Validate() error {
	v, err := getValidator()
	if err != nil {
		return fmt.Errorf("getValidator: %w", err)
	}

	return v.Struct(m)
}
