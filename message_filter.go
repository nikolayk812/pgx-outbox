package outbox

import "fmt"

type MessageFilter struct {
	EventTypes []string `validate:"required"`
	Brokers    []string `validate:"required"`
	Topics     []string `validate:"required"`
}

func (m *MessageFilter) Validate() error {
	v, err := getValidator()
	if err != nil {
		return fmt.Errorf("getValidator: %w", err)
	}

	return v.Struct(m)
}
