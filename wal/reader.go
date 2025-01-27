package wal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	outbox "github.com/nikolayk812/pgx-outbox"
)

const (
	outputPlugin = "pgoutput"

	defaultStandbyTimeout = time.Second * 10
	defaultChannelBuffer  = 10_000

	ConnectionStrReplicationDatabaseParam = "replication=database"
)

type Reader struct {
	connStr string
	conn    *pgconn.PgConn

	table         string
	publication   string
	slot          string
	permanentSlot bool

	onceStart sync.Once
	onceClose sync.Once

	standbyTimeout      time.Duration
	nextStandbyDeadline time.Time

	xLogPos   pglogrepl.LSN
	relations map[uint32]*pglogrepl.RelationMessageV2
	typeMap   *pgtype.Map

	channelBuffer int
	rawMessages   chan RawMessage
}

func NewReader(connStr, table, publication, slot string, opts ...ReadOption) (*Reader, error) {
	if !strings.Contains(connStr, ConnectionStrReplicationDatabaseParam) {
		// pglogrepl.IdentifySystem() call requires replication=database parameter
		return nil, ErrConnectionStrReplicationDatabaseParamAbsent
	}
	if table == "" {
		return nil, outbox.ErrTableEmpty
	}
	if publication == "" {
		return nil, ErrPublicationEmpty
	}
	if slot == "" {
		return nil, ErrReplicationSlotEmpty
	}

	r := &Reader{
		connStr:        connStr,
		table:          table,
		publication:    publication,
		slot:           slot,
		standbyTimeout: defaultStandbyTimeout,
		relations:      map[uint32]*pglogrepl.RelationMessageV2{},
		typeMap:        pgtype.NewMap(),
		channelBuffer:  defaultChannelBuffer,
	}

	for _, opt := range opts {
		opt(r)
	}

	r.rawMessages = make(chan RawMessage, r.channelBuffer)

	return r, nil
}

func (r *Reader) Start(ctx context.Context) (<-chan RawMessage, error) {
	var onceErr error

	r.onceStart.Do(func() {
		onceErr = r.start(ctx)
	})
	if onceErr != nil {
		return nil, fmt.Errorf("start: %w", onceErr)
	}

	go func() {
		if err := r.startLoop(ctx); err != nil {
			slog.Error("startLoop", "error", err)
		}
	}()

	return r.rawMessages, nil
}

func (r *Reader) start(ctx context.Context) error {
	if err := r.connect(ctx); err != nil {
		return fmt.Errorf("connect: %w", err)
	}

	publicationExists, err := r.publicationExists(ctx)
	if err != nil {
		return fmt.Errorf("publicationExists: %w", err)
	}

	if !publicationExists {
		if err := r.createPublication(ctx); err != nil {
			return fmt.Errorf("createPublication: %w", err)
		}
	}

	if err := r.startReplication(ctx); err != nil {
		return fmt.Errorf("startReplication: %w", err)
	}

	return nil
}

func (r *Reader) connect(ctx context.Context) error {
	conn, err := pgconn.Connect(ctx, r.connStr)
	if err != nil {
		return fmt.Errorf("pgconn.Connect: %w", err)
	}

	r.conn = conn

	return nil
}

func (r *Reader) createPublication(ctx context.Context) error {
	query := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s WITH (publish = 'insert')", r.publication, r.table)

	result := r.conn.Exec(ctx, query)
	defer closeResource("create_publication_query_result", result)

	_, err := result.ReadAll()
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			if pgErr.Code == "42P01" { // SQLSTATE for "relation does not exist"
				return ErrTableNotFound
			}
		}
		return fmt.Errorf("result.ReadAll: %w", err)
	}

	return nil
}

// it is not checking other fields: table name, pubinsert, pubupdate, pubdelete, etc.
func (r *Reader) publicationExists(ctx context.Context) (bool, error) {
	query := fmt.Sprintf("SELECT pubname FROM pg_publication WHERE pubname = '%s'", r.publication)

	result := r.conn.Exec(ctx, query)
	defer closeResource("publication_exists_query_result", result)

	row, err := toRow(result)
	if err != nil {
		return false, fmt.Errorf("toRow: %w", err)
	}

	if len(row) == 0 {
		return false, nil
	}

	return true, nil
}

//nolint:nonamedreturns
func (r *Reader) replicationSlotExists(ctx context.Context) (exists bool, active bool, _ error) {
	query := fmt.Sprintf("SELECT active FROM pg_replication_slots WHERE slot_name = '%s'", r.slot)

	result := r.conn.Exec(ctx, query)
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
		if _, err := pglogrepl.CreateReplicationSlot(ctx, r.conn, r.slot, outputPlugin,
			pglogrepl.CreateReplicationSlotOptions{Temporary: !r.permanentSlot}); err != nil {
			return fmt.Errorf("pglogrepl.CreateReplicationSlot: %w", err)
		}
	}

	sysIdent, err := pglogrepl.IdentifySystem(ctx, r.conn)
	if err != nil {
		return fmt.Errorf("pglogrepl.IdentifySystem: %w", err)
	}

	r.xLogPos = sysIdent.XLogPos

	pluginArguments := []string{
		"proto_version '2'", // pglogrepl does not support 3 or 4 at the moment
		fmt.Sprintf("publication_names '%s'", r.publication),
		"messages 'false'",  // pg_logical_emit_message() is not used
		"streaming 'false'", // receive only committed transactions
	}

	// no need to specify timeline, as 0 means current Postgres server timeline
	if err := pglogrepl.StartReplication(ctx, r.conn, r.slot, sysIdent.XLogPos,
		pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments}); err != nil {
		return fmt.Errorf("pglogrepl.StartReplication: %w", err)
	}

	return nil
}

func (r *Reader) startLoop(ctx context.Context) error {
	for {
		if err := r.sendStatusUpdate(ctx); err != nil {
			return fmt.Errorf("sendStatusUpdate: %w", err)
		}

		ctx, cancel := context.WithDeadline(ctx, r.nextStandbyDeadline)
		rawMsg, err := r.conn.ReceiveMessage(ctx)
		cancel() // cancel internal timer
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			return fmt.Errorf("conn.ReceiveMessage: %w", err)
		}

		msg, err := toCopyDataStruct(rawMsg)
		if err != nil {
			return fmt.Errorf("toCopyDataStruct: %w", err)
		}

		// msg.Data is guaranteed to have at least 2 bytes now, so [1:] is safe
		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			if err := r.handlePrimaryKeepalive(msg.Data[1:]); err != nil {
				return fmt.Errorf("handlePrimaryKeepalive: %w", err)
			}

		case pglogrepl.XLogDataByteID:
			if err := r.handleXLogData(msg.Data[1:]); err != nil {
				return fmt.Errorf("handleXLogData: %w", err)
			}
		}
	}
}

func (r *Reader) Close(ctx context.Context) error {
	var onceErr error

	r.onceClose.Do(func() {
		if r.conn != nil {
			onceErr = r.conn.Close(ctx)
		}

		close(r.rawMessages)
	})
	if onceErr != nil {
		return fmt.Errorf("conn.Close: %w", onceErr)
	}

	return nil
}
