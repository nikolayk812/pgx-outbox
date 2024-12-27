![CI Status](https://github.com/nikolayk812/pgx-outbox/actions/workflows/go.yml/badge.svg)
[![Go Report Card](https://goreportcard.com/badge/github.com/nikolayk812/pgx-outbox)](https://goreportcard.com/report/github.com/nikolayk812/pgx-outbox)
[![Go Reference](https://pkg.go.dev/badge/github.com/nikolayk812/pgx-outbox.svg)](https://pkg.go.dev/github.com/nikolayk812/pgx-outbox)
![License](https://img.shields.io/badge/license-MIT-blue.svg)
[![Coverage Status](https://coveralls.io/repos/github/nikolayk812/pgx-outbox/badge.svg)](https://coveralls.io/github/nikolayk812/pgx-outbox)

![Project Logo](./internal/logo.png)

# pgx-outbox

This is a simple outbox pattern implementation for PostgreSQL using pgx driver.

Motivation: avoid copy-pasting the same code in every project.

Not a general use-case queue



## How to use

### 1. Add database migration to a project:

```sql
CREATE TABLE IF NOT EXISTS outbox_messages
(
    id           BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    broker       TEXT                                NOT NULL,
    topic        TEXT                                NOT NULL,
    metadata     JSONB,
    payload      JSONB                               NOT NULL,

    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    published_at TIMESTAMP
);

CREATE INDEX idx_outbox_messages_published_at ON outbox_messages (published_at);
```

The outbox table name can be customized, but the table structure should remain exactly the same.

### 2. Add outbox.Writer to repository layer:

```go
type repo struct {
	pool *pgxpool.Pool
	
	writer outbox.Writer
	messageMapper types.ToMessageFunc[User]
}
```

To map your a domain model, i.e. `User` to the outbox message, implement the `types.ToMessageFunc` function is service layer and pass it to the repository either in `New` function or as a repository method parameter.

Start using the `writer.Write` method in the repository methods which should produce outbox messages.

```go
func (r *repo) CreateUser(ctx context.Context, user User) (u User, txErr error) {
	// create a transaction, commit/rollback in defer() depending on txErr

	user, err = r.createUser(ctx, tx, user)
	if err != nil {
		return u, fmt.Errorf("createUser: %w", err)
	}

	message, err := r.mapper(user)
	if err != nil {
		return u, fmt.Errorf("mapper: %w", err)
	}

	if _, err := r.writer.Write(ctx, tx, message); err != nil {
		return u, fmt.Errorf("writer.Write: %w", err)
	}

	return user, nil
}
```

See `outbox.Writer` example in [repo.go](./examples/01_sns/writer/repo.go) of the `01_sns` directory.


## Examples

please refer to the [examples/01_sns/README.md](examples/01_sns/README.md) file.

### Learning opportunities