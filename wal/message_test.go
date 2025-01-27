package wal_test

import (
	"testing"

	"github.com/nikolayk812/pgx-outbox/wal"
	"github.com/stretchr/testify/require"
)

func TestToOutboxMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		raw     wal.RawMessage
		wantErr string
	}{
		{
			name:    "invalid id type",
			raw:     wal.RawMessage{"id": "not-an-int64"},
			wantErr: "invalid field[id]: expected int64, got string with value [not-an-int64]",
		},
		{
			name:    "missing id",
			raw:     wal.RawMessage{},
			wantErr: "invalid field[id]: expected int64, got <nil> with value [<nil>]",
		},
		{
			name:    "invalid broker type",
			raw:     wal.RawMessage{"id": int64(1), "broker": 123},
			wantErr: "invalid field[broker]: expected string, got int with value [123]",
		},
		{
			name:    "missing broker",
			raw:     wal.RawMessage{"id": int64(1)},
			wantErr: "invalid field[broker]: expected string, got <nil> with value [<nil>]",
		},
		{
			name:    "invalid topic type",
			raw:     wal.RawMessage{"id": int64(1), "broker": "kafka", "topic": 123},
			wantErr: "invalid field[topic]: expected string, got int with value [123]",
		},
		{
			name:    "missing topic",
			raw:     wal.RawMessage{"id": int64(1), "broker": "kafka"},
			wantErr: "invalid field[topic]: expected string, got <nil> with value [<nil>]",
		},
		{
			name:    "invalid metadata type",
			raw:     wal.RawMessage{"id": int64(1), "broker": "kafka", "topic": "topic", "metadata": "not-a-byte-slice"},
			wantErr: "invalid field[metadata]: expected []byte, got string",
		},
		{
			name:    "invalid payload type",
			raw:     wal.RawMessage{"id": int64(1), "broker": "kafka", "topic": "topic", "payload": "not-a-byte-slice"},
			wantErr: "invalid field[payload]: expected []byte, got string",
		},
		{
			name:    "invalid metadata JSON",
			raw:     wal.RawMessage{"id": int64(1), "broker": "kafka", "topic": "topic", "metadata": []byte("invalid-json")},
			wantErr: "json.Unmarshal[metadata]:",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := tt.raw.ToOutboxMessage()
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
