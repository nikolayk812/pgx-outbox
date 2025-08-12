package wal_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
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
	tcNetwork "github.com/testcontainers/testcontainers-go/network"
)

const outboxTable = "outbox_messages"

var ctx = context.Background()

type ReaderTestSuite struct {
	suite.Suite
	pool              *pgxpool.Pool
	postgresContainer testcontainers.Container

	network            *testcontainers.DockerNetwork
	toxiProxyContainer testcontainers.Container
	toxiProxyProxy     *toxiproxy.Proxy

	writer outbox.Writer

	readerConnStr string
}

//nolint:paralleltest
func TestReaderTestSuite(t *testing.T) {
	suite.Run(t, new(ReaderTestSuite))
}

func (suite *ReaderTestSuite) SetupSuite() {
	// suite.noError(os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true"))

	network, err := tcNetwork.New(ctx)
	suite.noError(err)
	suite.network = network

	postgresContainer, connStr, err := containers.Postgres(ctx, "postgres:17.5-alpine3.22", network.Name)
	suite.noError(err)
	suite.postgresContainer = postgresContainer

	toxiProxyContainer, toxiProxyEndpoint, err := containers.ToxiProxy(ctx, "ghcr.io/shopify/toxiproxy:2.11.0", network.Name)
	suite.noError(err)
	suite.toxiProxyContainer = toxiProxyContainer

	toxiProxyCli := toxiproxy.NewClient(toxiProxyEndpoint)
	suite.Require().NotNil(toxiProxyCli)

	suite.toxiProxyProxy, err = toxiProxyCli.CreateProxy("postgres", "0.0.0.0:8666", "postgres:5432")
	suite.noError(err)
	suite.Require().NotNil(suite.toxiProxyProxy)

	suite.pool, err = pgxpool.New(ctx, connStr)
	suite.noError(err)

	suite.writer, err = outbox.NewWriter(outboxTable)
	suite.noError(err)

	connStr += "&" + wal.ConnectionStrReplicationDatabaseParam
	connStr = suite.proxiedConnStr(connStr)
	suite.readerConnStr = connStr
}

func (suite *ReaderTestSuite) TearDownSuite() {
	if suite.pool != nil {
		suite.pool.Close()
	}
	if suite.toxiProxyProxy != nil {
		suite.noError(suite.toxiProxyProxy.Delete())
	}
	if suite.postgresContainer != nil {
		suite.noError(suite.postgresContainer.Terminate(ctx))
	}
	if suite.toxiProxyContainer != nil {
		suite.noError(suite.toxiProxyContainer.Terminate(ctx))
	}
	if suite.network != nil {
		suite.noError(suite.network.Remove(ctx))
	}

	// goleak.VerifyNone(suite.T())
}

func (suite *ReaderTestSuite) TestReader_ReceiveMessages() {
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

			reader, err := wal.NewReader(suite.readerConnStr, outboxTable, "publication", "slot")
			require.NoError(t, err)

			msgCh, errCh, err := reader.Start(ctx)
			require.NoError(t, err)

			// GIVEN
			for _, message := range tt.in {
				_, err := suite.write(message)
				require.NoError(t, err)
			}

			var actual []types.Message
			// WHEN
			for rawMsg := range msgCh {
				message, err := rawMsg.ToOutboxMessage()
				require.NoError(t, err)

				actual = append(actual, message)

				if len(actual) == len(tt.in) {
					reader.Close()
				}
			}

			// THEN
			for err := range errCh {
				suite.noError(err)
			}

			assertEqualMessages(t, tt.in, actual)
		})
	}
}

func (suite *ReaderTestSuite) TestReader_ReceiveMessages_WithToxic() {
	msg1 := fakes.FakeMessage()
	// msg2 := fakes.FakeMessage()

	tests := []struct {
		name      string
		in        []types.Message
		toxic     toxiproxy.Toxic
		errSubStr string
	}{
		{
			name: "read_write_failure",
			in:   types.Messages{msg1},
			toxic: toxiproxy.Toxic{
				Name:     "read_write_failure",
				Type:     "timeout",
				Stream:   "downstream", // affects both directions
				Toxicity: 1.0,
				Attributes: toxiproxy.Attributes{
					"timeout": 0, // Timeout in milliseconds before closing the connection.
				},
			},
			errSubStr: "[closed]",
		},
		// Add more test cases as needed
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			t := suite.T()

			reader, err := wal.NewReader(suite.readerConnStr, outboxTable, "publication", "slot",
				wal.WithStandbyTimeout(100*time.Millisecond))
			require.NoError(t, err)

			_, errCh, err := reader.Start(ctx)
			require.NoError(t, err)

			toxic, err := suite.toxiProxyProxy.AddToxic(tt.toxic.Name,
				tt.toxic.Type, tt.toxic.Stream, tt.toxic.Toxicity, tt.toxic.Attributes)
			suite.noError(err)
			defer suite.noError(suite.toxiProxyProxy.RemoveToxic(toxic.Name))

			// THEN
			select {
			case err := <-errCh:
				assert.Contains(t, err.Error(), tt.errSubStr)
			case <-time.After(1 * time.Second):
				t.Fatal("timeout waiting for expected error")
			}
		})
	}
}

func (suite *ReaderTestSuite) TestReader_Start() {
	reader, err := wal.NewReader(suite.readerConnStr, outboxTable, "publication", "slot")
	suite.noError(err)
	defer reader.Close()

	_, _, err = reader.Start(ctx)
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
			defer reader2.Close()

			_, _, err = reader2.Start(ctx)
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

	require.Equal(t, len(expected), len(actual))

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

func (suite *ReaderTestSuite) proxiedConnStr(connStr string) string {
	pgPort, err := suite.postgresContainer.MappedPort(ctx, "5432/tcp")
	suite.noError(err)

	tpPort, err := suite.toxiProxyContainer.MappedPort(ctx, "8666/tcp")
	suite.noError(err)

	return strings.Replace(connStr, pgPort.Port(), tpPort.Port(), 1)
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
