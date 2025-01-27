package wal_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	outbox "github.com/nikolayk812/pgx-outbox"
	"github.com/nikolayk812/pgx-outbox/internal/containers"
	"github.com/nikolayk812/pgx-outbox/internal/fakes"
	"github.com/nikolayk812/pgx-outbox/types"
	"github.com/nikolayk812/pgx-outbox/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"go.uber.org/goleak"
)

const outboxTable = "outbox_messages"

var ctx = context.Background()

type ReaderTestSuite struct {
	suite.Suite
	pool      *pgxpool.Pool
	container testcontainers.Container

	writer outbox.Writer
	reader *wal.Reader

	readerConnStr string
}

//nolint:paralleltest
func TestReaderTestSuite(t *testing.T) {
	suite.Run(t, new(ReaderTestSuite))
}

func (suite *ReaderTestSuite) SetupSuite() {
	// suite.noError(os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true"))

	container, connStr, err := containers.Postgres(ctx, "postgres:17.2-alpine3.21", "")
	suite.noError(err)
	suite.container = container

	suite.pool, err = pgxpool.New(ctx, connStr)
	suite.noError(err)

	suite.writer, err = outbox.NewWriter(outboxTable)
	suite.noError(err)

	connStr += "&" + wal.ConnectionStrReplicationDatabaseParam
	suite.readerConnStr = connStr

	suite.reader, err = wal.NewReader(connStr, outboxTable, "publication", "slot")
	suite.noError(err)
}

func (suite *ReaderTestSuite) TearDownSuite() {
	if suite.reader != nil {
		suite.noError(suite.reader.Close(ctx))
	}
	if suite.pool != nil {
		suite.pool.Close()
	}
	if suite.container != nil {
		suite.NoError(suite.container.Terminate(ctx))
	}

	goleak.VerifyNone(suite.T())
}

func (suite *ReaderTestSuite) TestReader_ReceiveMessages() {
	ch, err := suite.reader.Start(ctx)
	suite.noError(err)

	msg1 := fakes.FakeMessage()
	msg2 := fakes.FakeMessage()
	msg3 := fakes.FakeMessage()

	tests := []struct {
		name string
		in   []types.Message
	}{
		{
			name: "single message",
			in:   types.Messages{msg1},
		},
		{
			name: "several messages",
			in:   types.Messages{msg1, msg2, msg3},
		},
		// Add more test cases as needed
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			t := suite.T()

			// GIVEN
			for _, message := range tt.in {
				_, err := suite.write(message)
				require.NoError(t, err)
			}

			var actual []types.Message
			// WHEN
			for i := 0; i < len(tt.in); i++ {
				rawMsg := <-ch

				message, err := rawMsg.ToOutboxMessage()
				require.NoError(t, err)

				actual = append(actual, message)
			}

			// THEN
			require.NoError(t, err)
			assertEqualMessages(t, tt.in, actual)
		})
	}
}

func (suite *ReaderTestSuite) TestReader_Start() {
	_, err := suite.reader.Start(ctx)
	suite.noError(err)

	tests := []struct {
		name        string
		connStr     string
		table       string
		publication string
		slot        string
		wantErr     error
	}{
		{
			name:        "replication slot is active",
			connStr:     suite.readerConnStr,
			table:       outboxTable,
			publication: "publication",
			slot:        "slot",
			wantErr:     wal.ErrReplicationSlotActive,
		},
		{
			name:        "same publication, different slot",
			connStr:     suite.readerConnStr,
			table:       outboxTable,
			publication: "publication",
			slot:        "slot2",
			wantErr:     nil,
		},
		{
			name:        "different publication, same slot",
			connStr:     suite.readerConnStr,
			table:       outboxTable,
			publication: "publication2",
			slot:        "slot",
			wantErr:     wal.ErrReplicationSlotActive,
		},
		{
			name:        "table does not exist",
			connStr:     suite.readerConnStr,
			table:       "outbox_messages3",
			publication: "publication3",
			slot:        "slot3",
			wantErr:     wal.ErrTableNotFound,
		},
		{
			name:        "cannot connect to database",
			connStr:     "postgres://postgres:postgres@localhost:2345/postgres?replication=database",
			table:       "outbox_messages4",
			publication: "publication4",
			slot:        "slot4",
			wantErr:     &pgconn.ConnectError{}, // cannot access err field which is unexported
		},
		{
			name:        "different publication, different slot",
			connStr:     suite.readerConnStr,
			table:       outboxTable,
			publication: "publication5",
			slot:        "slot5",
			wantErr:     nil,
		},
		// Add more test cases as needed
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			t := suite.T()

			reader2, err := wal.NewReader(tt.connStr, tt.table, tt.publication, tt.slot)
			require.NoError(t, err)
			defer reader2.Close(ctx)

			_, err = reader2.Start(ctx)
			if tt.wantErr != nil {
				var connErr *pgconn.ConnectError
				if errors.As(tt.wantErr, &connErr) {
					require.ErrorAs(t, err, &connErr) // special case
				} else {
					require.ErrorIs(t, err, tt.wantErr) // normal case
				}
				return
			}

			require.NoError(t, err)
		})
	}
}

type payload struct {
	Content string `json:"content"`
	Name    string `json:"name"`
}

func assertEqualMessage(t *testing.T, expected, actual types.Message) {
	t.Helper()

	cmpOptions := cmp.Options{
		cmp.FilterPath(func(p cmp.Path) bool {
			return p.Last().String() == ".ID"
		}, cmp.Ignore()),
		cmp.Comparer(func(x, y []byte) bool {
			var xp, yp payload
			if err := json.Unmarshal(x, &xp); err != nil {
				return false
			}
			if err := json.Unmarshal(y, &yp); err != nil {
				return false
			}
			// Compare unmarshalled objects instead of raw bytes
			return cmp.Equal(xp, yp)
		}),
	}

	diff := cmp.Diff(expected, actual, cmpOptions)
	assert.Equal(t, "", diff)
}

func assertEqualMessages(t *testing.T, expected, actual []types.Message) {
	t.Helper()

	assert.Equal(t, len(expected), len(actual))

	for i, e := range expected {
		assertEqualMessage(t, e, actual[i])
	}
}

func (suite *ReaderTestSuite) beginTx(ctx context.Context) (pgx.Tx, func(err error) error, error) {
	emptyFunc := func(_ error) error { return nil }

	tx, err := suite.pool.Begin(ctx)
	if err != nil {
		return nil, emptyFunc, fmt.Errorf("pool.Begin: %w", err)
	}

	commitFunc := func(execErr error) error {
		if execErr != nil {
			rbErr := tx.Rollback(ctx)
			if rbErr != nil {
				return fmt.Errorf("tx.Rollback %v: %w", execErr, rbErr) //nolint:errorlint
			}
			return execErr
		}

		txErr := tx.Commit(ctx)
		if txErr != nil {
			return fmt.Errorf("tx.Commit: %w", txErr)
		}

		return nil
	}

	return tx, commitFunc, nil
}

func (suite *ReaderTestSuite) write(message types.Message) (_ int64, txErr error) {
	tx, commitFunc, err := suite.beginTx(ctx)
	if err != nil {
		return 0, fmt.Errorf("beginTx: %w", err)
	}
	defer func() {
		if err := commitFunc(txErr); err != nil {
			txErr = fmt.Errorf("commitFunc: %w", err)
		}
	}()

	id, err := suite.writer.Write(ctx, tx, message)
	if err != nil {
		return 0, fmt.Errorf("writer.Write: %w", err)
	}

	return id, nil
}

func (suite *ReaderTestSuite) noError(err error) {
	suite.T().Helper()
	suite.Require().NoError(err)
}

// TestReader_New is just to increase coverage.
func (suite *ReaderTestSuite) TestReader_New() {
	tests := []struct {
		name        string
		connStr     string
		table       string
		publication string
		slot        string
		options     []wal.ReadOption
		wantErr     error
	}{
		{
			name:    "connection string replication database param absent",
			connStr: "",
			wantErr: wal.ErrConnectionStrReplicationDatabaseParamAbsent,
		},
		{
			name:    "empty table",
			connStr: "&replication=database",
			table:   "",
			wantErr: outbox.ErrTableEmpty,
		},
		{
			name:        "empty publication",
			connStr:     "?replication=database",
			table:       "outbox_messages",
			publication: "",
			wantErr:     wal.ErrPublicationEmpty,
		},
		{
			name:        "empty replication slot",
			connStr:     "?replication=database",
			table:       "outbox_messages",
			publication: "publication",
			slot:        "",
			wantErr:     wal.ErrReplicationSlotEmpty,
		},
		{
			name:        "with all options",
			connStr:     "?replication=database",
			table:       "outbox_messages",
			publication: "publication",
			slot:        "slot",
			options: []wal.ReadOption{
				wal.WithPermanentSlot(),
				wal.WithChannelBuffer(10),
				wal.WithStandbyTimeout(time.Second),
			},
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			t := suite.T()

			reader, err := wal.NewReader(tt.connStr, tt.table, tt.publication, tt.slot, tt.options...)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)
			assert.NotNil(t, reader)
		})
	}
}
