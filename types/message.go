package types

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

type Messages []Message

func (m Messages) IDs() []int64 {
	ids := make([]int64, 0, len(m))
	for _, msg := range m {
		ids = append(ids, msg.ID)
	}

	return ids
}

type ToMessageFunc[T any] func(entity T) (Message, error)
