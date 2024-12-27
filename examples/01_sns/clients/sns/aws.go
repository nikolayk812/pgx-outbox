package sns

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"
)

func NewAwsClient(ctx context.Context, region, endpoint string) (*sns.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region), config.WithBaseEndpoint(endpoint))
	if err != nil {
		return nil, fmt.Errorf("config.LoadDefaultConfig: %w", err)
	}

	cli := sns.NewFromConfig(cfg)
	if cli == nil {
		return nil, fmt.Errorf("sns.NewFromConfig returned nil")
	}

	return cli, nil
}
