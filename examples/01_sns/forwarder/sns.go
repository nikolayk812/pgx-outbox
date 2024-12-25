package main

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"
)

func createSnsClient(ctx context.Context, endpoint string) (*sns.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region), config.WithBaseEndpoint(endpoint))
	if err != nil {
		return nil, fmt.Errorf("config.LoadDefaultConfig: %w", err)
	}

	cli := sns.NewFromConfig(cfg)
	if cli == nil {
		return nil, fmt.Errorf("sns.NewFromConfig returned nil")
	}

	if _, err := cli.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String(topic),
	}); err != nil {
		return nil, fmt.Errorf("cli.CreateTopic: %w", err)
	}

	return cli, nil
}
