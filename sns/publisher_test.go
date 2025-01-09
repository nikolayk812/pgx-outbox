package sns_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsSns "github.com/aws/aws-sdk-go-v2/service/sns"
	outbox "github.com/nikolayk812/pgx-outbox"
	"github.com/nikolayk812/pgx-outbox/internal/containers"
	"github.com/nikolayk812/pgx-outbox/internal/fakes"
	"github.com/nikolayk812/pgx-outbox/sns"
	"github.com/nikolayk812/pgx-outbox/sns/internal/sqs"
	"github.com/nikolayk812/pgx-outbox/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
)

const (
	region = "eu-central-1"
	topic  = "topic1"
)

var ctx = context.Background()

type PublisherTestSuite struct {
	suite.Suite
	container testcontainers.Container
	sqsClient sqs.Client

	publisher outbox.Publisher
}

//nolint:paralleltest
func TestPublisherTestSuite(t *testing.T) {
	suite.Run(t, new(PublisherTestSuite))
}

func (suite *PublisherTestSuite) SetupSuite() {
	os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")

	container, endpoint, err := containers.Localstack(ctx, "localstack/localstack:4.0.3", "sns,sqs", "")
	suite.noError(err)
	suite.container = container

	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region), config.WithBaseEndpoint(endpoint),
		// GitHub Actions build fails without StaticCredentialsProvider
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "test")))
	suite.noError(err)

	awsSnsCli := awsSns.NewFromConfig(cfg)
	suite.Require().NotNil(awsSnsCli)

	suite.sqsClient, err = sqs.New(cfg)
	suite.noError(err)

	transformer := simpleTransformer{}

	suite.publisher, err = sns.NewPublisher(awsSnsCli, transformer)
	suite.noError(err)
}

func (suite *PublisherTestSuite) TearDownSuite() {
	if suite.container != nil {
		if err := suite.container.Terminate(ctx); err != nil {
			slog.Error("suite.container.Terminate", slog.Any("error", err))
		}
	}
}

func (suite *PublisherTestSuite) TestPublisher_Publish() {
	msg1 := fakes.FakeMessage()
	msg1.Topic = fmt.Sprintf("arn:aws:sns:%s:000000000000:%s", region, topic)

	queueURL, err := suite.sqsClient.GetQueueURL(ctx, "queue1")
	suite.noError(err)

	tests := []struct {
		name    string
		message types.Message
		wantErr bool
	}{
		{
			name:    "Publish message successfully",
			message: msg1,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			t := suite.T()

			// Call the method
			err := suite.publisher.Publish(ctx, tt.message)

			// Assert expectations
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			// receive message from SQS
			sqsMessage, err := suite.sqsClient.ReadOneFromSQS(ctx, queueURL, time.Second)
			require.NoError(t, err)

			// extract outbox payload from SQS message
			outboxPayload, err := suite.sqsClient.ExtractOutboxPayload(sqsMessage)
			require.NoError(t, err)

			assert.Equal(t, msg1.Payload, outboxPayload)
		})
	}
}

func (suite *PublisherTestSuite) noError(err error) {
	suite.Require().NoError(err)
}

type simpleTransformer struct{}

func (t simpleTransformer) Transform(_ context.Context, message types.Message) (*awsSns.PublishInput, error) {
	return &awsSns.PublishInput{
		Message:  aws.String(string(message.Payload)),
		TopicArn: &message.Topic,
	}, nil
}

func TestPublisher_New(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		snsClient   *awsSns.Client
		transformer sns.MessageTransformer
		expectedErr error
	}{
		{
			name:        "nil SNS client",
			snsClient:   nil,
			transformer: simpleTransformer{},
			expectedErr: sns.ErrSnsClientNil,
		},
		{
			name:        "nil transformer",
			snsClient:   &awsSns.Client{},
			transformer: nil,
			expectedErr: sns.ErrTransformerNil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			publisher, err := sns.NewPublisher(tt.snsClient, tt.transformer)
			assert.Nil(t, publisher)
			assert.ErrorIs(t, err, tt.expectedErr)
		})
	}
}
