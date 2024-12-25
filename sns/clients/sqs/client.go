package sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-lambda-go/events"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsSqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type Client interface {
	CreateQueue(ctx context.Context, queueName string) (qURL string, qARN string, err error)
	ReadOneFromSQS(ctx context.Context, queueUrl string, timeout time.Duration) (types.Message, error)
	ExtractOutboxPayload(message types.Message) ([]byte, error)
}

type client struct {
	cli *awsSqs.Client
}

func New(cfg aws.Config) (Client, error) {
	cli := awsSqs.NewFromConfig(cfg)
	if cli == nil {
		return nil, fmt.Errorf("sqs.NewFromConfig returned nil")
	}

	return &client{cli: cli}, nil
}

func (c *client) CreateQueue(ctx context.Context, queueName string) (qURL string, qARN string, _ error) {
	createOutput, err := c.cli.CreateQueue(ctx, &awsSqs.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		return "", "", fmt.Errorf("cli.CreateQueue: %w", err)
	}
	if createOutput == nil {
		return "", "", fmt.Errorf("cli.CreateQueue: output is nil")
	}
	if createOutput.QueueUrl == nil {
		return "", "", fmt.Errorf("cli.CreateQueue: output.QueueUrl is nil")
	}

	queueUrl := *createOutput.QueueUrl

	// Get the queue ARN which is weirdly not part of CreateQueue output
	attributesOutput, err := c.cli.GetQueueAttributes(ctx, &awsSqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueUrl),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		return "", "", fmt.Errorf("cli.GetQueueAttributes: %w", err)
	}
	if attributesOutput == nil {
		return "", "", fmt.Errorf("cli.GetQueueAttributes: output is nil")
	}

	queueArn := attributesOutput.Attributes[string(types.QueueAttributeNameQueueArn)]

	return queueUrl, queueArn, nil
}

func (c *client) ReadOneFromSQS(ctx context.Context, queueUrl string, timeout time.Duration) (m types.Message, _ error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return m, ctx.Err()
		default:
			messages, err := c.cli.ReceiveMessage(ctx, &awsSqs.ReceiveMessageInput{
				QueueUrl:            aws.String(queueUrl),
				MaxNumberOfMessages: 1,
			})
			if err != nil {
				return m, fmt.Errorf("cli.ReceiveMessage: %w", err)
			}

			if len(messages.Messages) == 0 {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			message := messages.Messages[0]

			_, err = c.cli.DeleteMessage(ctx, &awsSqs.DeleteMessageInput{
				QueueUrl:      aws.String(queueUrl),
				ReceiptHandle: message.ReceiptHandle,
			})
			if err != nil {
				return m, fmt.Errorf("cli.DeleteMessage: %w", err)
			}

			return message, nil
		}
	}
}

func (c *client) ExtractOutboxPayload(message types.Message) ([]byte, error) {
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
