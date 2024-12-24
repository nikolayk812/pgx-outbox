package outbox

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"testing"

	"github.com/nikolayk812/pgx-outbox/containers"
	"github.com/nikolayk812/pgx-outbox/fakes"
	"github.com/nikolayk812/pgx-outbox/types"

	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"go.uber.org/goleak"
)

var ctx = context.Background()

type WriterReaderTestSuite struct {
	suite.Suite
	pool      *pgxpool.Pool
	container testcontainers.Container

	writer Writer
	reader Reader
}

func TestWriterReaderTestSuite(t *testing.T) {
	suite.Run(t, new(WriterReaderTestSuite))
}

func (suite *WriterReaderTestSuite) SetupSuite() {
	container, connStr, err := containers.Postgres(ctx, "postgres:17.2-alpine3.21")
	suite.noError(err)
	suite.container = container

	suite.pool, err = pgxpool.New(ctx, connStr)
	suite.noError(err)

	err = suite.prepareDB("sql/01_outbox_messages.up.sql")
	suite.noError(err)

	suite.writer, err = NewWriter("outbox_messages") // TODO: const
	suite.noError(err)

	suite.reader, err = NewReader(suite.pool, "outbox_messages") // TODO: const
	suite.noError(err)
}

func (suite *WriterReaderTestSuite) TearDownSuite() {
	if suite.pool != nil {
		suite.pool.Close()
	}
	if suite.container != nil {
		if err := suite.container.Terminate(ctx); err != nil {
			slog.Error("suite.container.Terminate", slog.Any("error", err))
		}
	}

	goleak.VerifyNone(suite.T())
}

func (suite *WriterReaderTestSuite) TestWriter_WriteMessage() {
	invalidMessage := fakes.FakeMessage()
	invalidMessage.Broker = ""

	tests := []struct {
		name    string
		in      []types.Message
		wantErr bool
	}{
		{
			name: "no messages",
			in:   []types.Message{},
		},
		{
			name: "single message",
			in: []types.Message{
				fakes.FakeMessage(),
			},
		},
		{
			name: "multiple in",
			in: []types.Message{
				fakes.FakeMessage(),
				fakes.FakeMessage(),
			},
		},
		{
			name: "invalid message",
			in: []types.Message{
				invalidMessage,
			},
			wantErr: true,
		},
		// Add more test cases as needed
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			t := suite.T()

			// GIVEN
			for _, message := range tt.in {
				id, err := suite.write(message)
				if tt.wantErr {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				assert.Greater(t, id, int64(0))
			}

			limit := maxInt(1, len(tt.in))

			// THEN
			actual, err := suite.reader.Read(ctx, types.MessageFilter{}, limit)
			require.NoError(t, err)
			assertEqualMessages(t, tt.in, actual)

			suite.markAll()
		})
	}
}

func (suite *WriterReaderTestSuite) TestReader_ReadMessage() {
	msg1 := fakes.FakeMessage()
	msg2 := fakes.FakeMessage()
	msg3 := fakes.FakeMessage()

	tests := []struct {
		name    string
		in      []types.Message
		filter  types.MessageFilter
		limit   int
		out     []types.Message
		wantErr bool
	}{
		{
			name:    "limit 0",
			wantErr: true,
		},
		{
			name:  "single message",
			in:    types.Messages{msg1},
			limit: 1,
			out:   types.Messages{msg1},
		},
		{
			name:  "limit works and reader gets just one message",
			in:    types.Messages{msg1, msg2},
			limit: 1,
			out:   types.Messages{msg1},
		},
		{
			name:  "limit works if not enough in",
			in:    types.Messages{msg1, msg2},
			limit: 3,
			out:   types.Messages{msg1, msg2},
		},
		{
			name:   "filter by broker works",
			in:     types.Messages{msg1, msg2, msg3},
			filter: types.MessageFilter{Brokers: []string{msg2.Broker, msg3.Broker}},
			limit:  3,
			out:    types.Messages{msg2, msg3},
		}, {
			name:   "filter by topic works",
			in:     types.Messages{msg1, msg2},
			filter: types.MessageFilter{Topics: []string{msg1.Topic, msg2.Topic}},
			limit:  3,
			out:    types.Messages{msg1, msg2},
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

			// WHEN
			actual, err := suite.reader.Read(ctx, tt.filter, tt.limit)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			// THEN
			require.NoError(t, err)
			assertEqualMessages(t, tt.out, actual)

			suite.markAll()
		})
	}
}

func (suite *WriterReaderTestSuite) TestWriter_MarkMessage() {
	msg1 := fakes.FakeMessage()
	msg2 := fakes.FakeMessage()
	msg3 := fakes.FakeMessage()

	tests := []struct {
		name      string
		in        []types.Message
		count     int
		duplicate bool
	}{
		{
			name: "nothing to mark",
			in:   types.Messages{},
		},
		{
			name:  "single message",
			in:    types.Messages{msg1},
			count: 1,
		},
		{
			name:      "single message duplicate",
			in:        types.Messages{msg1},
			count:     1,
			duplicate: true,
		},
		{
			name:  "one of two marked",
			in:    types.Messages{msg1, msg2},
			count: 1,
		},
		{
			name:  "two of three marked",
			in:    types.Messages{msg1, msg2, msg3},
			count: 2,
		},
		{
			name:  "three of three marked",
			in:    types.Messages{msg1, msg2, msg3},
			count: 3,
		},
		{
			name:  "three of three duplicate",
			in:    types.Messages{msg1, msg2, msg3},
			count: 3,
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			t := suite.T()

			// GIVEN
			var ids []int64

			for _, message := range tt.in {
				id, err := suite.write(message)
				require.NoError(t, err)
				ids = append(ids, id)
			}

			require.GreaterOrEqual(t, len(ids), tt.count)

			// WHEN
			idsToMark := ids[:tt.count]

			if tt.duplicate {
				idsToMark = append(idsToMark, idsToMark...)
			}

			affected, err := suite.reader.Ack(ctx, idsToMark)

			// THEN
			require.NoError(t, err)
			assert.Equal(t, tt.count, int(affected))

			// WHEN-2
			limit := maxInt(1, len(tt.in))

			actual, err := suite.reader.Read(ctx, types.MessageFilter{}, limit)

			// THEN-2
			require.NoError(t, err)
			assert.Len(t, actual, len(tt.in)-tt.count) // how many are left unmarked

			suite.markAll()
		})
	}
}

func TestMain(m *testing.M) {
	os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")

	os.Exit(m.Run())
}

type payload struct {
	Content string `json:"content"`
}

//go:embed sql/*.sql
var sqlFiles embed.FS

func (suite *WriterReaderTestSuite) prepareDB(scriptPath string) error {
	script, err := sqlFiles.ReadFile(scriptPath)
	if err != nil {
		return fmt.Errorf("sqlFiles.ReadFile: %w", err)
	}

	if _, err := suite.pool.Exec(ctx, string(script)); err != nil {
		return fmt.Errorf("pool.Exec: %w", err)
	}

	return nil
}

func assertEqualMessage(t *testing.T, expected, actual types.Message) {
	cmpOptions := cmp.Options{
		cmp.FilterPath(func(p cmp.Path) bool {
			return p.Last().String() == ".ID"
		}, cmp.Ignore()),
		// TODO: explain
		cmp.Comparer(func(x, y []byte) bool {
			var xp, yp payload
			if err := json.Unmarshal(x, &xp); err != nil {
				return false
			}
			if err := json.Unmarshal(y, &yp); err != nil {
				return false
			}
			return cmp.Equal(xp, yp)
		}),
	}

	diff := cmp.Diff(expected, actual, cmpOptions)
	assert.Equal(t, "", diff)
}

func assertEqualMessages(t *testing.T, expected, actual []types.Message) {
	assert.Equal(t, len(expected), len(actual))

	for i, e := range expected {
		assertEqualMessage(t, e, actual[i])
	}
}

func (suite *WriterReaderTestSuite) beginTx(ctx context.Context) (pgx.Tx, func(err error) error, error) {
	emptyFunc := func(err error) error { return nil }

	tx, err := suite.pool.Begin(ctx)
	if err != nil {
		return nil, emptyFunc, fmt.Errorf("pool.Begin: %w", err)
	}

	commitFunc := func(execErr error) error {
		if execErr != nil {
			rbErr := tx.Rollback(ctx)
			if rbErr != nil {
				return fmt.Errorf("tx.Rollback %v: %w", execErr, rbErr)
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

func (suite *WriterReaderTestSuite) write(message types.Message) (id int64, txErr error) {
	tx, commitFunc, err := suite.beginTx(ctx)
	if err != nil {
		return 0, fmt.Errorf("beginTx: %w", err)
	}
	defer func() {
		if err := commitFunc(txErr); err != nil {
			txErr = fmt.Errorf("commitFunc: %w", err)
		}
	}()

	if id, err = suite.writer.Write(ctx, tx, message); err != nil {
		return 0, fmt.Errorf("writer.Write: %w", err)
	}

	return id, nil
}

func (suite *WriterReaderTestSuite) markAll() {
	emptyFilter := types.MessageFilter{}
	maxLimit := 100

	actual, err := suite.reader.Read(ctx, emptyFilter, maxLimit)
	suite.noError(err)

	ids := types.Messages(actual).IDs()
	affected, err := suite.reader.Ack(ctx, ids)
	suite.noError(err)
	suite.Len(ids, int(affected))

	// THEN nothing cannot be found anymore
	actual, err = suite.reader.Read(ctx, emptyFilter, maxLimit)
	suite.noError(err)
	suite.Len(actual, 0)
}

func (suite *WriterReaderTestSuite) noError(err error) {
	suite.Require().NoError(err)
}

func maxInt(x, y int) int {
	if x > y {
		return x
	}
	return y
}
