package sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsSqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqsTypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/nikolayk812/pgx-outbox/types"
)

type Client interface {
	GetQueueURL(ctx context.Context, queueName string) (string, error)
	ReadOneFromSQS(ctx context.Context, queueUrl string, timeout time.Duration) (sqsTypes.Message, error)
	ToOutboxMessage(message sqsTypes.Message) (types.Message, error)
}

type client struct {
	cli *awsSqs.Client
}

func New(cli *awsSqs.Client) (Client, error) {
	if cli == nil {
		return nil, fmt.Errorf("cli is nil")
	}

	return &client{cli: cli}, nil
}

func (c *client) GetQueueURL(ctx context.Context, queueName string) (string, error) {
	output, err := c.cli.GetQueueUrl(ctx, &awsSqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		return "", fmt.Errorf("cli.GetQueueUrl: %w", err)
	}
	if output == nil || output.QueueUrl == nil {
		return "", fmt.Errorf("cli.GetQueueUrl: output or QueueUrl is nil")
	}

	return *output.QueueUrl, nil
}

func (c *client) ReadOneFromSQS(ctx context.Context, queueUrl string, timeout time.Duration) (m sqsTypes.Message, _ error) {
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

func (c *client) ToOutboxMessage(message sqsTypes.Message) (m types.Message, _ error) {
	if message.Body == nil {
		return m, fmt.Errorf("message.Body is nil")
	}

	var snsMsg events.SNSEntity
	if err := json.Unmarshal([]byte(*message.Body), &snsMsg); err != nil {
		return m, fmt.Errorf("json.Unmarshal: %w", err)
	}

	if snsMsg.Type != "Notification" {
		return m, fmt.Errorf("snsMsg.Type is not Notification: [%s]", snsMsg.Type)
	}

	metadata := map[string]string{}

	for k, v := range snsMsg.MessageAttributes {
		if v == nil {
			continue
		}

		if mam, ok := v.(map[string]interface{}); ok {
			// another key is Type, strange structure
			if str, ok := mam["Value"].(string); ok {
				metadata[k] = str
			}
		}
	}

	return types.Message{
		Broker:   "sns",
		Topic:    snsMsg.TopicArn,
		Metadata: metadata,
		Payload:  []byte(snsMsg.Message),
	}, nil
}
