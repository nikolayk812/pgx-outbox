package types

import (
	"fmt"
)

type Message struct {
	// ID is assigned by Postgres database after calling a Write method of outbox.Writer.
	ID int64

	// Broker is the name of the message broker, i.e. "kafka", "sns", etc.
	Broker string `validate:"required"`

	// Topic is the name or id (i.e. AWS ARN) of the message topic where the message will be published.
	Topic string `validate:"required"`

	// Metadata is optional map to implement outbox.Publisher interface more flexibly.
	Metadata map[string]string

	// Payload is the message body, ideally it should be published as is, but can be transformed in outbox.Publisher.
	Payload []byte `validate:"required,json"`
}

func (m *Message) Validate() error {
	v, err := getValidator()
	if err != nil {
		return fmt.Errorf("getValidator: %w", err)
	}

	return v.Struct(m)
}

type Messages []Message

func (m Messages) IDs() []int64 {
	ids := make([]int64, 0, len(m))

	for _, msg := range m {
		ids = append(ids, msg.ID)
	}

	return ids
}

func (m Messages) Validate() error {
	for idx, msg := range m {
		if err := msg.Validate(); err != nil {
			return fmt.Errorf("msg.Validate idx[%d]: %w", idx, err)
		}
	}

	return nil
}

type ToMessageFunc[T any] func(entity T) (Message, error)
