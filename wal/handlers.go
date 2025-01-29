package wal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

func (r *Reader) sendStatusUpdate(ctx context.Context) error {
	now := time.Now()

	if now.Before(r.nextStandbyDeadline) {
		return nil
	}

	err := pglogrepl.SendStandbyStatusUpdate(ctx, r.getConn(), pglogrepl.StandbyStatusUpdate{WALWritePosition: r.xLogPos})
	if err != nil {
		return fmt.Errorf("pglogrepl.SendStandbyStatusUpdate: %w", err)
	}

	r.nextStandbyDeadline = now.Add(r.standbyTimeout)

	return nil
}

func toCopyDataStruct(rawMsg pgproto3.BackendMessage) (*pgproto3.CopyData, error) {
	if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
		return nil, fmt.Errorf("pgproto3.ErrorResponse: %+v", errMsg)
	}

	msg, ok := rawMsg.(*pgproto3.CopyData)
	if !ok {
		return nil, fmt.Errorf("type[%T]: %w", rawMsg, ErrUnexpectedMessageType)
	}

	if len(msg.Data) <= 2 {
		return nil, fmt.Errorf("pgproto3.CopyData data is too short [%d]", len(msg.Data))
	}

	return msg, nil
}

func (r *Reader) handlePrimaryKeepalive(data []byte) error {
	pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(data)
	if err != nil {
		return fmt.Errorf("pglogrepl.ParsePrimaryKeepaliveMessage: %w", err)
	}

	if pkm.ServerWALEnd > r.xLogPos {
		// looks weird but Logical replication clients don’t need to process every byte of the WAL,
		// as they only care about specific changes (e.g., those related to a publication).
		r.xLogPos = pkm.ServerWALEnd
	}

	if pkm.ReplyRequested {
		r.nextStandbyDeadline = time.Now() // for immediate heartbeat to Postgres server
	}

	return nil
}

func (r *Reader) handleXLogData(data []byte) error {
	xld, err := pglogrepl.ParseXLogData(data)
	if err != nil {
		return fmt.Errorf("pglogrepl.ParseXLogData: %w", err)
	}

	// log.Printf("XLogData => WALStart %s ServerWALEnd %s ServerTime %s WALData:\n",
	//	xld.WALStart, xld.ServerWALEnd, xld.ServerTime)

	if err := r.processV2(xld.WALData); err != nil {
		return fmt.Errorf("processV2: %w", err)
	}

	if xld.WALStart > r.xLogPos {
		r.xLogPos = xld.WALStart
	}

	return nil
}

func (r *Reader) processV2(walData []byte) error {
	logicalMsg, err := pglogrepl.ParseV2(walData, false)
	if err != nil {
		return fmt.Errorf("pglogrepl.ParseV2: %w", err)
	}

	switch msg := logicalMsg.(type) {
	case *pglogrepl.RelationMessageV2:
		r.relations[msg.RelationID] = msg

	case *pglogrepl.InsertMessageV2:
		rawMessage, err := r.handleInsert(msg)
		if err != nil {
			return fmt.Errorf("handleInsert: %w", err)
		}

		select {
		case r.messageCh <- rawMessage:
			return nil
		default:
			return fmt.Errorf("messageCh channel is full")
		}
	}

	return nil
}

func (r *Reader) handleInsert(msg *pglogrepl.InsertMessageV2) (RawMessage, error) {
	if msg.Tuple == nil {
		return nil, fmt.Errorf("msg.Tuple is nil")
	}

	rawMessage := RawMessage{}

	for idx, col := range msg.Tuple.Columns {
		column, err := r.getRelationColumn(msg.RelationID, idx)
		if err != nil {
			return nil, fmt.Errorf("getRelationColumn[%d]: %w", msg.RelationID, err)
		}

		switch col.DataType {
		case 'n': // null
			rawMessage[column.Name] = nil
		case 'u': // unchanged toast
			// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
		case 't': // text
			val, err := r.decodeTextColumnData(col.Data, column.DataType)
			if err != nil {
				return nil, fmt.Errorf("decodeTextColumnData[%s]: %w", column.Name, err)
			}
			rawMessage[column.Name] = val
		}
	}

	return rawMessage, nil
}

// guaranteed to be non-nil if error is nil.
func (r *Reader) getRelationColumn(relationID uint32, idx int) (*pglogrepl.RelationMessageColumn, error) {
	rel, ok := r.relations[relationID]
	if !ok {
		return nil, errors.New("unknown relation")
	}

	if idx >= len(rel.Columns) {
		return nil, fmt.Errorf("column index[%d] is out of range[%d]", idx, len(rel.Columns))
	}

	column := rel.Columns[idx]
	if column == nil {
		return nil, fmt.Errorf("column[%d] is nil", idx)
	}

	return column, nil
}

func (r *Reader) decodeTextColumnData(data []byte, dataType uint32) (interface{}, error) {
	// If the data type is JSONB, return it as []byte
	if dataType == pgtype.JSONBOID {
		return data, nil
	}

	if dt, ok := r.typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(r.typeMap, dataType, pgtype.TextFormatCode, data)
	}

	return string(data), nil
}

func toRow(reader *pgconn.MultiResultReader) ([][]byte, error) {
	if reader == nil {
		return nil, errors.New("reader is nil")
	}

	results, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("result.ReadAll: %w", err)
	}

	if len(results) == 0 {
		return nil, nil
	}

	if results[0].Err != nil {
		return nil, fmt.Errorf("results[0].Err: %w", results[0].Err)
	}

	if len(results[0].Rows) == 0 {
		return nil, nil
	}

	return results[0].Rows[0], nil
}

func closeResource(name string, resource io.Closer) {
	if resource == nil {
		return
	}

	if err := resource.Close(); err != nil {
		slog.Error("closeResource", "name", name, "error", err)
	}
}
