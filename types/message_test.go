package types_test

import (
	"testing"

	outbox "github.com/nikolayk812/pgx-outbox/types"
	"github.com/stretchr/testify/require"
)

func TestMessage_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		message outbox.Message
		wantErr bool
	}{
		{
			name: "valid message",
			message: outbox.Message{
				Broker:  "sns",
				Topic:   "topic",
				Payload: []byte(`{"content":"test"}`),
			},
			wantErr: false,
		},
		{
			name: "missing broker",
			message: outbox.Message{
				Topic:   "topic",
				Payload: []byte(`{"content":"test"}`),
			},
			wantErr: true,
		},
		{
			name: "missing topic",
			message: outbox.Message{
				Broker:  "sns",
				Payload: []byte(`{"content":"test"}`),
			},
			wantErr: true,
		},
		{
			name: "invalid payload",
			message: outbox.Message{
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
			t.Parallel()

			err := tt.message.Validate()
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
		})
	}
}
