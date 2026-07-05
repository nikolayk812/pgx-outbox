package outbox_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	outbox "github.com/nikolayk812/pgx-outbox"
	"github.com/nikolayk812/pgx-outbox/internal/containers"
	"github.com/nikolayk812/pgx-outbox/internal/fakes"
	"github.com/nikolayk812/pgx-outbox/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"go.uber.org/goleak"
)

const outboxTable = "outbox_messages"

var ctx = context.Background()

type WriterReaderTestSuite struct {
	suite.Suite
	pool      *pgxpool.Pool
	db        *sql.DB
	container testcontainers.Container

	writer outbox.Writer
	reader outbox.Reader
}

//nolint:paralleltest
func TestWriterReaderTestSuite(t *testing.T) {
	suite.Run(t, new(WriterReaderTestSuite))
}

func (suite *WriterReaderTestSuite) SetupSuite() {
	suite.noError(os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true"))

	container, connStr, err := containers.Postgres(ctx, "postgres:18.3-alpine3.23", "")
	suite.noError(err)
	suite.container = container

	suite.pool, err = pgxpool.New(ctx, connStr)
	suite.noError(err)

	suite.db, err = sql.Open("pgx", connStr)
	suite.noError(err)

	suite.writer, err = outbox.NewWriter(outboxTable)
	suite.noError(err)

	suite.reader, err = outbox.NewReader(outboxTable, suite.pool)
	suite.noError(err)
}

func (suite *WriterReaderTestSuite) TearDownSuite() {
	if suite.pool != nil {
		suite.pool.Close()
	}
	if suite.db != nil {
		suite.NoError(suite.db.Close())
	}
	if suite.container != nil {
		suite.NoError(suite.container.Terminate(ctx))
	}

	goleak.VerifyNone(suite.T())
}

//nolint:dupl
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
				assert.Positive(t, id)
			}

			limit := maxInt(1, len(tt.in))

			// THEN
			actual, err := suite.reader.Read(ctx, limit)
			require.NoError(t, err)
			assertEqualMessages(t, tt.in, actual)

			suite.markAll()
		})
	}
}

//nolint:dupl
func (suite *WriterReaderTestSuite) TestWriter_WriteMessageStdLib() {
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
				id, err := suite.writeStdLib(message)
				if tt.wantErr {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				assert.Positive(t, id)
			}

			limit := maxInt(1, len(tt.in))

			// THEN
			actual, err := suite.reader.Read(ctx, limit)
			require.NoError(t, err)
			assertEqualMessages(t, tt.in, actual)

			suite.markAll()
		})
	}
}

func (suite *WriterReaderTestSuite) TestWriter_WriteMessagesBatch() {
	invalidMessage := fakes.FakeMessage()
	invalidMessage.Broker = ""

	tests := []struct {
		name    string
		in      []types.Message
		wantLen int
		wantErr bool
	}{
		{
			name:    "no messages",
			in:      []types.Message{},
			wantLen: 0,
		},
		{
			name: "single message",
			in: []types.Message{
				fakes.FakeMessage(),
			},
			wantLen: 1,
		},
		{
			name: "two in",
			in: []types.Message{
				fakes.FakeMessage(),
				fakes.FakeMessage(),
			},
			wantLen: 2,
		},
		{
			name: "five in",
			in: []types.Message{
				fakes.FakeMessage(),
				fakes.FakeMessage(),
				fakes.FakeMessage(),
				fakes.FakeMessage(),
				fakes.FakeMessage(),
			},
			wantLen: 5,
		},
		{
			name: "invalid message",
			in: []types.Message{
				invalidMessage,
			},
			wantLen: 0,
			wantErr: true,
		},
		// Add more test cases as needed
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			t := suite.T()

			// GIVEN

			ids, err := suite.writeBatch(tt.in)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Len(t, ids, tt.wantLen)

			limit := maxInt(1, len(tt.in))

			// THEN
			actual, err := suite.reader.Read(ctx, limit)
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
		},
		{
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

			reader, err := outbox.NewReader(outboxTable, suite.pool, outbox.WithReadFilter(tt.filter))
			require.NoError(t, err)

			// WHEN
			actual, err := reader.Read(ctx, tt.limit)
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

func (suite *WriterReaderTestSuite) TestWriter_AckMessage() {
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
			name:      "three of three duplicate",
			in:        types.Messages{msg1, msg2, msg3},
			count:     3,
			duplicate: true,
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

			acked, err := suite.reader.Ack(ctx, idsToMark)

			// THEN
			require.NoError(t, err)
			assert.Len(t, acked, tt.count)

			// WHEN-2
			limit := maxInt(1, len(tt.in))

			actual, err := suite.reader.Read(ctx, limit)

			// THEN-2
			require.NoError(t, err)
			assert.Len(t, actual, len(tt.in)-tt.count) // how many are left unmarked

			suite.markAll()
		})
	}
}

type payload struct {
	Content string `json:"content"`
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

func (suite *WriterReaderTestSuite) beginTx(ctx context.Context) (pgx.Tx, func(err error) error, error) {
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

func (suite *WriterReaderTestSuite) beginTxStdLib() (*sql.Tx, func(err error) error, error) {
	emptyFunc := func(_ error) error { return nil }

	tx, err := suite.db.Begin()
	if err != nil {
		return nil, emptyFunc, fmt.Errorf("db.Begin: %w", err)
	}

	commitFunc := func(execErr error) error {
		if execErr != nil {
			rbErr := tx.Rollback()
			if rbErr != nil {
				return fmt.Errorf("tx.Rollback %v: %w", execErr, rbErr) //nolint:errorlint
			}
			return execErr
		}

		txErr := tx.Commit()
		if txErr != nil {
			return fmt.Errorf("tx.Commit: %w", txErr)
		}

		return nil
	}

	return tx, commitFunc, nil
}

func (suite *WriterReaderTestSuite) write(message types.Message) (_ int64, txErr error) {
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

func (suite *WriterReaderTestSuite) writeStdLib(message types.Message) (_ int64, txErr error) {
	tx, commitFunc, err := suite.beginTxStdLib()
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

func (suite *WriterReaderTestSuite) writeBatch(messages []types.Message) (_ []int64, txErr error) {
	tx, commitFunc, err := suite.beginTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginTx: %w", err)
	}
	defer func() {
		if err := commitFunc(txErr); err != nil {
			txErr = fmt.Errorf("commitFunc: %w", err)
		}
	}()

	ids, err := suite.writer.WriteBatch(ctx, tx, messages)
	if err != nil {
		return nil, fmt.Errorf("writer.Write: %w", err)
	}

	return ids, nil
}

func (suite *WriterReaderTestSuite) markAll() {
	suite.T().Helper()

	maxLimit := 100

	actual, err := suite.reader.Read(ctx, maxLimit)
	suite.noError(err)

	ids := types.Messages(actual).IDs()
	acked, err := suite.reader.Ack(ctx, ids)
	suite.noError(err)
	suite.Equal(ids, acked)

	// THEN nothing cannot be found anymore
	actual, err = suite.reader.Read(ctx, maxLimit)
	suite.noError(err)
	suite.Empty(actual)
}

func (suite *WriterReaderTestSuite) noError(err error) {
	suite.T().Helper()
	suite.Require().NoError(err)
}

//nolint:unparam
func maxInt(x, y int) int {
	if x > y {
		return x
	}
	return y
}

// TestWriter_New is just to increase coverage.
func (suite *WriterReaderTestSuite) TestWriter_New() {
	tests := []struct {
		name    string
		table   string
		options []outbox.WriteOption
		wantErr error
	}{
		{
			name:    "empty table",
			table:   "",
			wantErr: outbox.ErrTableEmpty,
		},
		{
			name:  "non-empty table",
			table: "outbox_messages",
		},
		{
			name:    "with options",
			table:   "outbox_messages",
			options: []outbox.WriteOption{outbox.WithDisablePreparedBatch()},
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			t := suite.T()

			writer, err := outbox.NewWriter(tt.table, tt.options...)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)
			assert.NotNil(t, writer)
		})
	}
}

func (suite *WriterReaderTestSuite) TestWriter_WriteWithNilTx() {
	message := fakes.FakeMessage()

	tests := []struct {
		name    string
		writeFn func() error
		wantErr error
	}{
		{
			name: "Write with nil tx",
			writeFn: func() error {
				_, err := suite.writer.Write(ctx, nil, message)
				return err
			},
			wantErr: outbox.ErrTxNil,
		},
		{
			name: "Write with unsupported tx type",
			writeFn: func() error {
				_, err := suite.writer.Write(ctx, struct{}{}, message)
				return err
			},
			wantErr: outbox.ErrTxUnsupportedType,
		},
		{
			name: "WriteBatch with nil tx",
			writeFn: func() error {
				_, err := suite.writer.WriteBatch(ctx, nil, []types.Message{message})
				return err
			},
			wantErr: outbox.ErrTxNil,
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			t := suite.T()

			err := tt.writeFn()
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

// TestReader_New is just to increase coverage.
func (suite *WriterReaderTestSuite) TestReader_New() {
	tests := []struct {
		name    string
		table   string
		pool    *pgxpool.Pool
		options []outbox.ReadOption
		wantErr error
	}{
		{
			name:    "empty table",
			table:   "",
			pool:    suite.pool,
			wantErr: outbox.ErrTableEmpty,
		},
		{
			name:    "nil pool",
			table:   "outbox_messages",
			pool:    nil,
			wantErr: outbox.ErrPoolNil,
		},
		{
			name:    "with options",
			table:   "outbox_messages",
			pool:    suite.pool,
			options: []outbox.ReadOption{outbox.WithReadFilter(types.MessageFilter{Brokers: []string{"broker1"}})},
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			t := suite.T()

			reader, err := outbox.NewReader(tt.table, tt.pool, tt.options...)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)
			assert.NotNil(t, reader)
		})
	}
}

type recordingPublisher struct {
	published []types.Message
}

func (p *recordingPublisher) Publish(_ context.Context, message types.Message) error {
	p.published = append(p.published, message)
	return nil
}

func (suite *WriterReaderTestSuite) TestForwarder_FromPool_ForwardsAndAcksMessages() {
	t := suite.T()

	// GIVEN
	msg1 := fakes.FakeMessage()
	msg2 := fakes.FakeMessage()

	_, err := suite.write(msg1)
	require.NoError(t, err)

	_, err = suite.write(msg2)
	require.NoError(t, err)

	publisher := &recordingPublisher{}
	forwarder, err := outbox.NewForwarderFromPool(outboxTable, suite.pool, publisher)
	require.NoError(t, err)

	// WHEN
	out, err := forwarder.Forward(ctx, 10)

	// THEN
	require.NoError(t, err)
	assert.Len(t, out.Read, 2)
	assert.Len(t, out.PublishedIDs, 2)
	assert.Len(t, out.AckedIDs, 2)
	assert.Len(t, publisher.published, 2)

	assertEqualMessages(t, []types.Message{msg1, msg2}, publisher.published)

	remaining, err := suite.reader.Read(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, remaining, 0)
}

func (suite *WriterReaderTestSuite) TestForwarder_FromPool_ConcurrentWorkers_DoNotDoubleRead() {
	t := suite.T()

	// GIVEN
	total := 6
	for i := 0; i < total; i++ {
		_, err := suite.write(fakes.FakeMessage())
		require.NoError(t, err)
	}

	pub1 := &recordingPublisher{}
	pub2 := &recordingPublisher{}

	fwd1, err := outbox.NewForwarderFromPool(outboxTable, suite.pool, pub1)
	require.NoError(t, err)

	fwd2, err := outbox.NewForwarderFromPool(outboxTable, suite.pool, pub2)
	require.NoError(t, err)

	type result struct {
		out types.ForwardOutput
		err error
	}

	ch := make(chan result, 2)

	// WHEN
	go func() {
		out, err := fwd1.Forward(ctx, total)
		ch <- result{out: out, err: err}
	}()

	go func() {
		out, err := fwd2.Forward(ctx, total)
		ch <- result{out: out, err: err}
	}()

	r1 := <-ch
	r2 := <-ch

	// THEN
	require.NoError(t, r1.err)
	require.NoError(t, r2.err)

	allPublished := append(pub1.published, pub2.published...)
	assert.Len(t, allPublished, total)

	seen := make(map[int64]int)
	for _, msg := range allPublished {
		seen[msg.ID]++
	}

	assert.Len(t, seen, total)
	for id, count := range seen {
		assert.Equalf(t, 1, count, "message id %d was published more than once", id)
	}

	remaining, err := suite.reader.Read(ctx, total)
	require.NoError(t, err)
	assert.Len(t, remaining, 0)
}

type stubReader struct {
	readCalled bool
	ackCalled  bool
	messages   []types.Message
	ackedIDs   []int64
}

func (r *stubReader) Read(_ context.Context, limit int) ([]types.Message, error) {
	r.readCalled = true
	if limit < len(r.messages) {
		return r.messages[:limit], nil
	}
	return r.messages, nil
}

func (r *stubReader) Ack(_ context.Context, ids []int64) ([]int64, error) {
	r.ackCalled = true
	r.ackedIDs = append(r.ackedIDs, ids...)
	return ids, nil
}

func (suite *WriterReaderTestSuite) TestForwarder_NonPoolReader_FallbackPathStillWorks() {
	t := suite.T()

	// GIVEN
	msg1 := fakes.FakeMessage()
	msg1.ID = 101

	msg2 := fakes.FakeMessage()
	msg2.ID = 102

	reader := &stubReader{
		messages: []types.Message{msg1, msg2},
	}

	publisher := &recordingPublisher{}

	forwarder, err := outbox.NewForwarder(reader, publisher)
	require.NoError(t, err)

	// WHEN
	out, err := forwarder.Forward(ctx, 10)

	// THEN
	require.NoError(t, err)
	assert.True(t, reader.readCalled)
	assert.True(t, reader.ackCalled)

	assert.Len(t, out.Read, 2)
	assert.Len(t, out.PublishedIDs, 2)
	assert.Len(t, out.AckedIDs, 2)
	assert.Len(t, publisher.published, 2)

	assert.Equal(t, []int64{101, 102}, out.PublishedIDs)
	assert.Equal(t, []int64{101, 102}, out.AckedIDs)
	assert.Equal(t, []int64{101, 102}, reader.ackedIDs)
}

