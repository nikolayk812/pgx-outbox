package wal

import (
	"context"
	"fmt"

	"github.com/jackc/pglogrepl"
)

//nolint:nonamedreturns
func (r *Reader) replicationSlotExists(ctx context.Context) (exists bool, active bool, _ error) {
	query := fmt.Sprintf("SELECT active FROM pg_replication_slots WHERE slot_name = '%s'", r.slot)

	result := r.getConn().Exec(ctx, query)
	defer closeResource("replication_slot_exists_query_result", result)

	row, err := toRow(result)
	if err != nil {
		return false, false, fmt.Errorf("toRow: %w", err)
	}

	if len(row) == 0 {
		return false, false, nil
	}

	if len(row[0]) > 0 {
		// 't' is true, 'f' is false
		return true, row[0][0] == 't', nil
	}

	return true, false, nil
}

func (r *Reader) startReplication(ctx context.Context) error {
	exists, active, err := r.replicationSlotExists(ctx)
	if err != nil {
		return fmt.Errorf("replicationSlotExists: %w", err)
	}
	if active {
		return ErrReplicationSlotActive
	}

	if !exists {
		if _, err := pglogrepl.CreateReplicationSlot(ctx, r.getConn(), r.slot, outputPlugin,
			pglogrepl.CreateReplicationSlotOptions{Temporary: !r.permanentSlot}); err != nil {
			return fmt.Errorf("pglogrepl.CreateReplicationSlot: %w", err)
		}
	}

	sysIdent, err := pglogrepl.IdentifySystem(ctx, r.getConn())
	if err != nil {
		return fmt.Errorf("pglogrepl.IdentifySystem: %w", err)
	}

	r.lastReceivedLSN = sysIdent.XLogPos
	r.updateLastProcessedLSN(sysIdent.XLogPos)

	pluginArguments := []string{
		"proto_version '2'", // pglogrepl does not support 3 or 4 at the moment
		fmt.Sprintf("publication_names '%s'", r.publication),
		"messages 'false'",  // pg_logical_emit_message() is not used
		"streaming 'false'", // receive only committed transactions
	}

	// no need to specify timeline, as 0 means current Postgres server timeline
	if err := pglogrepl.StartReplication(ctx, r.getConn(), r.slot, sysIdent.XLogPos,
		pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments}); err != nil {
		return fmt.Errorf("pglogrepl.StartReplication: %w", err)
	}

	return nil
}
