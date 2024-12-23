package sns

import (
	"context"
	"github.com/testcontainers/testcontainers-go"
	"log/slog"
	"outbox/containers"
	"outbox/fakes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awsSns "github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"outbox"
)

const (
	region    = "eu-central-1"
	topicArn  = "arn:aws:sns:eu-central-1:000000000000:topic1"
	topicName = "topic1"
)

var ctx = context.Background()

type PublisherTestSuite struct {
	suite.Suite
	container testcontainers.Container

	publisher outbox.Publisher
}

func TestPublisherTestSuite(t *testing.T) {
	suite.Run(t, new(PublisherTestSuite))
}

func (suite *PublisherTestSuite) SetupSuite() {
	container, endpoint, err := containers.Localstack(ctx, "localstack/localstack:latest")
	suite.noError(err)
	suite.container = container

	snsClient, err := suite.createClient(endpoint)
	suite.noError(err)

	transformer := simpleTransformer{}

	suite.publisher, err = NewPublisher(snsClient, transformer)
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
	msg1.Topic = topicArn

	tests := []struct {
		name    string
		message outbox.Message
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
		})
	}
}

func (suite *PublisherTestSuite) noError(err error) {
	suite.Require().NoError(err)
}

func (suite *PublisherTestSuite) createClient(endpoint string) (*awsSns.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	suite.noError(err)

	cli := awsSns.NewFromConfig(cfg, func(o *awsSns.Options) {
		o.BaseEndpoint = &endpoint
	})

	// Create SNS topic
	_, err = cli.CreateTopic(ctx, &awsSns.CreateTopicInput{
		Name: aws.String(topicName),
	})
	suite.noError(err)

	return cli, nil
}

type simpleTransformer struct{}

func (t simpleTransformer) Transform(message outbox.Message) (*awsSns.PublishInput, error) {
	return &awsSns.PublishInput{
		Message:  aws.String(string(message.Payload)),
		TopicArn: &message.Topic,
	}, nil
}
