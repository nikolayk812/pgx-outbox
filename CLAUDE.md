# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This is `pgx-outbox`, a Go library implementing the transactional outbox pattern for PostgreSQL using the jackc/pgx driver. It solves the dual writes problem by ensuring database writes and message publishing happen atomically.

## Core Architecture

The library consists of several key components:

- **Writer (`outbox.Writer`)**: Writes messages to the outbox table within database transactions
- **Forwarder (`outbox.Forwarder`)**: Reads unpublished messages from outbox table and publishes them via a Publisher
- **Reader (`outbox.Reader`)**: Reads messages from outbox table and marks them as published
- **Publisher (`outbox.Publisher`)**: Interface for publishing messages to external systems (SNS implementation provided)

### Key Types

- `types.Message`: Core message structure with ID, Broker, Topic, Metadata, and Payload
- `types.MessageFilter`: Filters messages by broker/topic in the Reader
- `types.ForwardOutput`: Contains statistics about forwarding operations

### Directory Structure

- `/types`: Core types and message definitions
- `/sns`: AWS SNS publisher implementation  
- `/wal`: Write-Ahead Log (WAL) based implementation for reading changes
- `/examples`: Complete working examples (01_sns, 02_wal, 03_debezium, 04_peerdb)

## Development Commands

### Building and Testing
```bash
# Build the project
make build

# Run tests (includes docker-build for examples)
make test

# Run tests with coverage and open in browser
make cover

# Run tests with coverage for CI
make cover-ci
```

### Code Quality
```bash
# Run linter and formatter
make lint

# Fix formatting issues
make fix-lint

# Generate code
make generate

# Pre-push checks (runs tests)
make push-check
```

### Docker Operations
```bash
# Build docker images for examples
make docker-build
```

## Testing Strategy

- Tests use testcontainers for PostgreSQL and LocalStack (AWS services emulation)
- Environment variable `TESTCONTAINERS_RYUK_DISABLED=true` is used to disable container cleanup
- Coverage excludes internal and examples packages
- Tests run with race detection enabled

## Code Style and Linting

The project uses golangci-lint with extensive configuration including:
- Code correctness: errcheck, govet, staticcheck, unused
- Code quality: cyclop, funlen, gocritic, gocyclo
- Code style: gofmt, gofumpt, goimports, revive
- Best practices: paralleltest, tenv, testpackage

## Examples

The `/examples` directory contains complete working implementations:

### 01_sns Example
- **Writer**: Creates fake users and writes to outbox table
- **Forwarder**: Reads from outbox and publishes to SNS via LocalStack
- **SQS Reader**: Consumes messages from SQS queue

Run example:
```bash
cd examples/01_sns
docker-compose up -d
go run ./writer     # terminal 1
go run ./forwarder  # terminal 2  
go run ./sqs_reader # terminal 3
```

### Other Examples
- `02_wal`: WAL-based change capture
- `03_debezium`: Debezium CDC integration
- `04_peerdb`: PeerDB integration

## Database Schema

The outbox table structure is fixed:
```sql
CREATE TABLE IF NOT EXISTS outbox_messages (
    id           BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    broker       TEXT NOT NULL,
    topic        TEXT NOT NULL, 
    metadata     JSONB,
    payload      JSONB NOT NULL,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    published_at TIMESTAMP
);
```

## Usage Patterns

1. **In Repository Layer**: Use `outbox.Writer` to write messages within database transactions
2. **In Background Jobs**: Use `outbox.Forwarder` to publish unpublished messages (typically in cronjobs)
3. **Message Mapping**: Implement `types.ToMessageFunc[T]` to convert domain objects to outbox messages
4. **Custom Publishers**: Implement `outbox.Publisher` interface for different message brokers

## Important Notes

- Run only one Forwarder instance per outbox table to avoid conflicts
- Failed message publishing blocks forwarder progress - monitor and handle poison messages
- Messages are published at-least-once (may be republished if acknowledgment fails)
- Use message filters to isolate different forwarder instances on the same table