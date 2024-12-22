CREATE TABLE IF NOT EXISTS outbox_messages
(
    id           BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    event_type   TEXT                                NOT NULL,
    broker       TEXT                                NOT NULL,
    topic        TEXT                                NOT NULL,
    payload      JSONB                               NOT NULL,

    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    published_at TIMESTAMP
);

CREATE INDEX idx_outbox_messages_published_at ON outbox_messages (published_at);