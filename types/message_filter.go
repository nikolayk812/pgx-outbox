package types

import "fmt"

// TODO: add metadata?
type MessageFilter struct {
	Brokers []string
	Topics  []string
}

func (m *MessageFilter) Validate() error {
	v, err := getValidator()
	if err != nil {
		return fmt.Errorf("getValidator: %w", err)
	}

	return v.Struct(m)
}
