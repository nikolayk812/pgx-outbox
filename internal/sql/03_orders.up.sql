CREATE TABLE IF NOT EXISTS orders
(
    id              UUID PRIMARY KEY,
    customer_name   VARCHAR(255)                        NOT NULL,
    items_count     INT                                 NOT NULL,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

