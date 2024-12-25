package sns

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsSns "github.com/aws/aws-sdk-go-v2/service/sns"
)

type Client interface {
	CreateTopic(ctx context.Context, topicName string) (topicARN string, err error)
	SubscribeQueueToTopic(ctx context.Context, queueARN, topicARN string) error
}

type client struct {
	cli *awsSns.Client
}

func New(cli *awsSns.Client) (Client, error) {
	if cli == nil {
		return nil, fmt.Errorf("sns.New: cli is nil")
	}

	return &client{cli: cli}, nil
}

func (c *client) CreateTopic(ctx context.Context, topicName string) (string, error) {
	output, err := c.cli.CreateTopic(ctx, &awsSns.CreateTopicInput{
		Name: aws.String(topicName),
	})
	if err != nil {
		return "", fmt.Errorf("cli.CreateTopic: %w", err)
	}
	if output == nil {
		return "", fmt.Errorf("cli.CreateTopic: output is nil")
	}
	if output.TopicArn == nil {
		return "", fmt.Errorf("cli.CreateTopic: output.TopicArn is nil")
	}

	return *output.TopicArn, nil
}

func (c *client) SubscribeQueueToTopic(ctx context.Context, queueARN, topicARN string) error {
	if _, err := c.cli.Subscribe(ctx, &awsSns.SubscribeInput{
		Protocol: aws.String("sqs"),
		TopicArn: aws.String(topicARN),
		Endpoint: aws.String(queueARN),
	}); err != nil {
		return fmt.Errorf("cli.Subscribe: %w", err)
	}

	return nil
}
