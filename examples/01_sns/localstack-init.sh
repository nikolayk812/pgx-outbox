#!/bin/bash

set -e

echo "Initializing LocalStack resources..."

# Dummy credentials for LocalStack
export AWS_ACCESS_KEY_ID="dummy"
export AWS_SECRET_ACCESS_KEY="dummy"

# Define resource names
SNS_TOPIC_NAME="topic1"
SQS_QUEUE_NAME="queue1"
LOCALSTACK_ENDPOINT="http://localhost:4566"
AWS_REGION="eu-central-1"

# Create the SNS topic
aws --endpoint-url=$LOCALSTACK_ENDPOINT sns create-topic --name $SNS_TOPIC_NAME --region $AWS_REGION

# Create the SQS queue
aws --endpoint-url=$LOCALSTACK_ENDPOINT sqs create-queue --queue-name $SQS_QUEUE_NAME --region $AWS_REGION

# Get the ARN for the SNS topic
SNS_TOPIC_ARN=$(aws --endpoint-url=$LOCALSTACK_ENDPOINT sns list-topics --region $AWS_REGION --query "Topics[?contains(TopicArn, '${SNS_TOPIC_NAME}')].TopicArn" --output text)

# Get the URL and ARN for the SQS queue
SQS_QUEUE_URL=$(aws --endpoint-url=$LOCALSTACK_ENDPOINT sqs get-queue-url --queue-name $SQS_QUEUE_NAME --region $AWS_REGION --query "QueueUrl" --output text)
SQS_QUEUE_ARN=$(aws --endpoint-url=$LOCALSTACK_ENDPOINT sqs get-queue-attributes --queue-url $SQS_QUEUE_URL --attribute-names QueueArn --region $AWS_REGION --query "Attributes.QueueArn" --output text)

# Subscribe the SQS queue to the SNS topic
aws --endpoint-url=$LOCALSTACK_ENDPOINT sns subscribe --topic-arn $SNS_TOPIC_ARN --protocol sqs --notification-endpoint $SQS_QUEUE_ARN --region $AWS_REGION

echo "LocalStack resources initialized successfully."