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

func TestMessages_IDs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		messages outbox.Messages
		expected []int64
	}{
		{
			name:     "nil",
			messages: nil,
			expected: []int64{},
		},
		{
			name:     "empty",
			messages: outbox.Messages{},
			expected: []int64{},
		},
		{
			name: "one message",
			messages: outbox.Messages{
				{ID: 1},
			},
			expected: []int64{1},
		},
		{
			name: "two messages",
			messages: outbox.Messages{
				{ID: 1},
				{ID: 2},
			},
			expected: []int64{1, 2},
		},
		{
			name: "duplicate IDs",
			messages: outbox.Messages{
				{ID: 1},
				{ID: 1},
			},
			expected: []int64{1, 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			actual := tt.messages.IDs()
			require.Equal(t, tt.expected, actual)
		})
	}
}
