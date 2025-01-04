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

-- https://www.postgresql.org/docs/current/indexes-partial.html
CREATE INDEX IF NOT EXISTS idx_outbox_messages_published_at_null ON outbox_messages (published_at) WHERE published_at IS NULL;