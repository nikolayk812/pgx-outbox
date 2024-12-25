package main

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"
)

func createSnsClient(ctx context.Context, endpoint string) (*sns.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("config.LoadDefaultConfig: %w", err)
	}

	cli := sns.NewFromConfig(cfg, func(o *sns.Options) {
		o.BaseEndpoint = &endpoint
	})

	if _, err := cli.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String(topic),
	}); err != nil {
		return nil, fmt.Errorf("cli.CreateTopic: %w", err)
	}

	return cli, nil
}
