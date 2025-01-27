package wal

import (
	"encoding/json"
	"fmt"

	"github.com/nikolayk812/pgx-outbox/types"
)

type RawMessage map[string]interface{}

//nolint:nonamedreturns
func (raw RawMessage) ToOutboxMessage() (m types.Message, _ error) {
	msg := types.Message{}

	rawID := raw["id"]
	if id, ok := rawID.(int64); ok {
		msg.ID = id
	} else {
		return m, fmt.Errorf("invalid field[id]: expected int64, got %T with value [%v]", rawID, rawID)
	}

	rawBroker := raw["broker"]
	if broker, ok := rawBroker.(string); ok {
		msg.Broker = broker
	} else {
		return m, fmt.Errorf("invalid field[broker]: expected string, got %T with value [%v]", rawBroker, rawBroker)
	}

	rawTopic := raw["topic"]
	if topic, ok := rawTopic.(string); ok {
		msg.Topic = topic
	} else {
		return m, fmt.Errorf("invalid field[topic]: expected string, got %T with value [%v]", rawTopic, rawTopic)
	}

	rawMetadata := raw["metadata"]
	if rawMetadata != nil {
		if bytesMetadata, ok := rawMetadata.([]byte); ok {
			if err := json.Unmarshal(bytesMetadata, &msg.Metadata); err != nil {
				return m, fmt.Errorf("json.Unmarshal[metadata]: %w", err)
			}
		} else {
			return m, fmt.Errorf("invalid field[metadata]: expected []byte, got %T", rawMetadata)
		}
	}

	rawPayload := raw["payload"]
	if payload, ok := rawPayload.([]byte); ok {
		msg.Payload = payload
	} else {
		return m, fmt.Errorf("invalid field[payload]: expected []byte, got %T", rawPayload)
	}

	return msg, nil
}
