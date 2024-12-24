package sns

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"
	"time"

	outbox "github.com/nikolayk812/pgx-outbox"

	"github.com/nikolayk812/pgx-outbox/containers"
	"github.com/nikolayk812/pgx-outbox/fakes"
	"github.com/nikolayk812/pgx-outbox/types"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqsTypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
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
	snsClient *awsSns.Client
	sqsClient *sqs.Client

	publisher outbox.Publisher
}

func TestPublisherTestSuite(t *testing.T) {
	suite.Run(t, new(PublisherTestSuite))
}

func (suite *PublisherTestSuite) SetupSuite() {
	container, endpoint, err := containers.Localstack(ctx, "localstack/localstack:latest")
	suite.noError(err)
	suite.container = container

	suite.snsClient, err = suite.createSnsClient(endpoint)
	suite.noError(err)

	suite.sqsClient, err = suite.createSqsClient(endpoint)
	suite.noError(err)

	transformer := simpleTransformer{}

	suite.publisher, err = NewPublisher(suite.snsClient, transformer)
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
	topicArn := suite.createTopic("topic1")
	queueURL, queueARN := suite.createQueue("queue1")
	suite.subscribeQueueToTopic(queueARN, topicArn)

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
			sqsMessage, err := suite.readFromSQS(queueURL, time.Second)
			require.NoError(t, err)

			// extract outbox payload from SQS message
			outboxPayload, err := extractOutboxPayload(sqsMessage)
			require.NoError(t, err)

			assert.Equal(t, msg1.Payload, outboxPayload)
		})
	}
}

func (suite *PublisherTestSuite) noError(err error) {
	suite.Require().NoError(err)
}

func (suite *PublisherTestSuite) createSnsClient(endpoint string) (*awsSns.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	suite.noError(err)

	cli := awsSns.NewFromConfig(cfg, func(o *awsSns.Options) {
		o.BaseEndpoint = &endpoint
	})

	return cli, nil
}

func (suite *PublisherTestSuite) createTopic(topic string) string {
	output, err := suite.snsClient.CreateTopic(ctx, &awsSns.CreateTopicInput{
		Name: aws.String(topic),
	})
	suite.noError(err)

	return *output.TopicArn
}

func (suite *PublisherTestSuite) createSqsClient(endpoint string) (*sqs.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	suite.noError(err)

	cli := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		o.BaseEndpoint = &endpoint
	})

	return cli, nil
}

func (suite *PublisherTestSuite) createQueue(queue string) (string, string) {
	createOutput, err := suite.sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queue),
	})
	suite.noError(err)

	queueUrl := createOutput.QueueUrl

	// Get the queue ARN which is weirdly not part of CreateQueue output
	attributesOutput, err := suite.sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(*queueUrl),
		AttributeNames: []sqsTypes.QueueAttributeName{sqsTypes.QueueAttributeNameQueueArn},
	})
	suite.noError(err)

	queueArn := attributesOutput.Attributes[string(sqsTypes.QueueAttributeNameQueueArn)]

	return *createOutput.QueueUrl, queueArn
}

func (suite *PublisherTestSuite) subscribeQueueToTopic(queueARN, topicARN string) {
	_, err := suite.snsClient.Subscribe(ctx, &awsSns.SubscribeInput{
		Protocol: aws.String("sqs"),
		TopicArn: aws.String(topicARN),
		Endpoint: aws.String(queueARN),
	})
	suite.noError(err)
}

func (suite *PublisherTestSuite) readFromSQS(
	queueUrl string,
	timeout time.Duration,
) (m sqsTypes.Message, _ error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return m, ctx.Err()
		default:
			messages, err := suite.sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
				QueueUrl:            aws.String(queueUrl),
				MaxNumberOfMessages: 1,
			})
			if err != nil {
				return m, fmt.Errorf("sqsClient.ReceiveMessage: %w", err)
			}

			if len(messages.Messages) == 0 {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			message := messages.Messages[0]

			_, err = suite.sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(queueUrl),
				ReceiptHandle: message.ReceiptHandle,
			})
			if err != nil {
				return m, fmt.Errorf("sqsClient.DeleteMessage: %w", err)
			}

			return message, nil
		}
	}
}

type simpleTransformer struct{}

func (t simpleTransformer) Transform(message types.Message) (*awsSns.PublishInput, error) {
	return &awsSns.PublishInput{
		Message:  aws.String(string(message.Payload)),
		TopicArn: &message.Topic,
	}, nil
}

func extractOutboxPayload(message sqsTypes.Message) ([]byte, error) {
	if message.Body == nil {
		return nil, fmt.Errorf("message.Body is nil")
	}

	var snsMsg events.SNSEntity
	if err := json.Unmarshal([]byte(*message.Body), &snsMsg); err != nil {
		return nil, fmt.Errorf("json.Unmarshal: %w", err)
	}

	if snsMsg.Type != "Notification" {
		return nil, fmt.Errorf("snsMsg.Type is not Notification: [%s]", snsMsg.Type)
	}

	return []byte(snsMsg.Message), nil
}
