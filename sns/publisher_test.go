package sns

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/nikolayk812/pgx-outbox/sns/clients/sns"
	"github.com/nikolayk812/pgx-outbox/sns/clients/sqs"

	outbox "github.com/nikolayk812/pgx-outbox"

	"github.com/nikolayk812/pgx-outbox/internal/containers"
	"github.com/nikolayk812/pgx-outbox/internal/fakes"
	"github.com/nikolayk812/pgx-outbox/types"

	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awsSns "github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const (
	region = "eu-central-1"
)

var ctx = context.Background()

type PublisherTestSuite struct {
	suite.Suite
	container testcontainers.Container
	snsClient sns.Client
	sqsClient sqs.Client

	publisher outbox.Publisher
}

func TestPublisherTestSuite(t *testing.T) {
	suite.Run(t, new(PublisherTestSuite))
}

func (suite *PublisherTestSuite) SetupSuite() {
	container, endpoint, err := containers.Localstack(ctx, "localstack/localstack:4.0.3")
	suite.noError(err)
	suite.container = container

	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region), config.WithBaseEndpoint(endpoint),
		// GitHub Actions build fails without StaticCredentialsProvider
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "test")))
	suite.noError(err)

	awsSnsCli := awsSns.NewFromConfig(cfg)
	suite.Require().NotNil(awsSnsCli)

	suite.snsClient, err = sns.New(awsSnsCli)
	suite.noError(err)

	suite.sqsClient, err = sqs.New(cfg)
	suite.noError(err)

	transformer := simpleTransformer{}

	suite.publisher, err = NewPublisher(awsSnsCli, transformer)
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
	topicArn, err := suite.snsClient.CreateTopic(ctx, "topic1")
	suite.noError(err)

	queueURL, queueARN, err := suite.sqsClient.CreateQueue(ctx, "queue1")
	suite.noError(err)

	suite.noError(suite.snsClient.SubscribeQueueToTopic(ctx, queueARN, topicArn))

	msg1 := fakes.FakeMessage()
	msg1.Topic = topicArn

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

func (t simpleTransformer) Transform(message types.Message) (*awsSns.PublishInput, error) {
	return &awsSns.PublishInput{
		Message:  aws.String(string(message.Payload)),
		TopicArn: &message.Topic,
	}, nil
}
