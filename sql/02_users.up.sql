CREATE TABLE IF NOT EXISTS users
(
    id         UUID PRIMARY KEY,
    name       VARCHAR(255)                        NOT NULL,
    age        INT                                 NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

