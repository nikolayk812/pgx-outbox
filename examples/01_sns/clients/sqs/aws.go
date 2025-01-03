package sqs

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func NewAwsClient(ctx context.Context, region, endpoint string) (*sqs.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region), config.WithBaseEndpoint(endpoint),
		// to prevent error [get identity: get credentials: failed to refresh cached credentials, no EC2 IMDS role found, operation error ec2imds].
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "test")))
	if err != nil {
		return nil, fmt.Errorf("config.LoadDefaultConfig: %w", err)
	}

	cli := sqs.NewFromConfig(cfg)
	if cli == nil {
		return nil, fmt.Errorf("sqs.NewFromConfig returned nil")
	}

	return cli, nil
}
