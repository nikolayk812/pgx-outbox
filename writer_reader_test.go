package outbox

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"github.com/brianvoe/gofakeit"
	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"go.uber.org/goleak"
	"log/slog"
	"os"
	"outbox/containers"
	"testing"
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
	suite.Require().NoError(err)
	suite.container = container

	suite.pool, err = pgxpool.New(ctx, connStr)
	suite.Require().NoError(err)

	err = suite.prepareDB("sql/01_outbox_messages.up.sql")
	suite.Require().NoError(err)

	suite.writer, err = NewWriter("outbox_messages") // TODO: const
	suite.Require().NoError(err)

	suite.reader, err = NewReader(suite.pool, "outbox_messages") // TODO: const
	suite.Require().NoError(err)
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

// TODO: rename
func (suite *WriterReaderTestSuite) TestPutEvent() {
	t := suite.T()

	tx, err := suite.pool.Begin(ctx)
	require.NoError(t, err)

	message := fakeMessage()

	err = suite.writer.Write(ctx, tx, message)
	require.NoError(t, err)

	err = tx.Commit(ctx)
	require.NoError(t, err)

	messages, err := suite.reader.Read(ctx, MessageFilter{}, 1)
	require.NoError(t, err)

	assert.Len(t, messages, 1)
	assertEqualMessage(t, message, messages[0])
}

func TestMain(m *testing.M) {
	os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")

	os.Exit(m.Run())
}

type payload struct {
	Content string `json:"content"`
}

// TODO: polish
func fakeMessage() Message {
	p := payload{Content: gofakeit.Quote()}

	pp, _ := json.Marshal(p)

	return Message{
		Broker:  "sns", // TODO:
		Topic:   "topic",
		Payload: pp,
	}
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

func assertEqualMessage(t *testing.T, expected, actual Message) {
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
