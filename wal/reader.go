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
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	outbox "github.com/nikolayk812/pgx-outbox"
)

const (
	outputPlugin = "pgoutput"

	defaultStandbyTimeout = time.Second * 10
	defaultChannelBuffer  = 10_000

	ConnectionStrReplicationDatabaseParam = "replication=database"
)

// Requirements:
// - Postgres 15 or higher
// -
//
// Limitations:
// - pgoutput built-in Postgres plugin only; wal2json, decoderbufs are not supported
// - single table per publication / replication slot
// - logical replication protocol v2 only; v3 and v4 are not supported
// - only insert operations are supported
// - pg_logical_emit_message() is not supported
// - transaction streaming is not supported
// - custom types are not supported

type Reader struct {
	connStr  string
	conn     *pgconn.PgConn
	connLock sync.Mutex // as pgconn.PgConn is not concurrency-safe

	table         string
	publication   string
	slot          string
	permanentSlot bool

	onceStart sync.Once
	onceClose sync.Once
	closeCh   chan struct{}

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
		closeCh:        make(chan struct{}),
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
		if onceErr != nil {
			return
		}

		go func() {
			if err := r.startLoop(ctx); err != nil {
				slog.Error("startLoop", "error", err)
			}
		}()
	})
	if onceErr != nil {
		return nil, fmt.Errorf("start: %w", onceErr)
	}

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

	r.setConn(conn)

	return nil
}

//nolint:cyclop
func (r *Reader) startLoop(ctx context.Context) error {
	for {
		if err := r.sendStatusUpdate(ctx); err != nil {
			return fmt.Errorf("sendStatusUpdate: %w", err)
		}

		var rawMsg pgproto3.BackendMessage

		select {
		case <-ctx.Done():
			return r.close(ctx)
		case <-r.closeCh:
			return r.close(ctx)
		default:
			var err error

			ctx, cancel := context.WithDeadline(ctx, r.nextStandbyDeadline)
			rawMsg, err = r.getConn().ReceiveMessage(ctx)
			cancel() // cancel internal timer
			if err != nil {
				if pgconn.Timeout(err) {
					continue
				}
				return fmt.Errorf("conn.ReceiveMessage: %w", err)
			}
		}

		msg, err := toCopyDataStruct(rawMsg)
		if err != nil {
			if errors.Is(err, ErrUnexpectedMessageType) {
				continue
			}
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

func (r *Reader) Close() {
	r.onceClose.Do(func() {
		close(r.closeCh)
	})
}

func (r *Reader) close(ctx context.Context) error {
	defer close(r.rawMessages)

	if err := r.getConn().Close(ctx); err != nil {
		return fmt.Errorf("conn.Close: %w", err)
	}

	return nil
}

func (r *Reader) getConn() *pgconn.PgConn {
	r.connLock.Lock()
	defer r.connLock.Unlock()

	return r.conn
}

func (r *Reader) setConn(conn *pgconn.PgConn) {
	r.connLock.Lock()
	defer r.connLock.Unlock()

	r.conn = conn
}
