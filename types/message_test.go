package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMessage_Validate(t *testing.T) {
	tests := []struct {
		name    string
		message Message
		wantErr bool
	}{
		{
			name: "valid message",
			message: Message{
				Broker:  "sns",
				Topic:   "topic",
				Payload: []byte(`{"content":"test"}`),
			},
			wantErr: false,
		},
		{
			name: "missing broker",
			message: Message{
				Topic:   "topic",
				Payload: []byte(`{"content":"test"}`),
			},
			wantErr: true,
		},
		{
			name: "missing topic",
			message: Message{
				Broker:  "sns",
				Payload: []byte(`{"content":"test"}`),
			},
			wantErr: true,
		},
		{
			name: "invalid payload",
			message: Message{
				Broker:  "sns",
				Topic:   "topic",
				Payload: []byte(`invalid`),
			},
			wantErr: true,
		},
		// Add more test cases as needed
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.message.Validate()
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
		})
	}
}
