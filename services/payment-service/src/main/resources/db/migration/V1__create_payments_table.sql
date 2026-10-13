CREATE SCHEMA IF NOT EXISTS payment_service;

CREATE TABLE payment_service.payments
(
    id                 UUID PRIMARY KEY,
    order_id           UUID NOT NULL,
    amount             NUMERIC(12, 2) NOT NULL CHECK (amount > 0),
    currency           VARCHAR(3) NOT NULL,
    status             VARCHAR(16) NOT NULL,
    provider_reference VARCHAR(255),
    failure_reason     VARCHAR(255),
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_payments_order_id UNIQUE (order_id),
    CONSTRAINT chk_payments_status CHECK (status IN ('PENDING', 'COMPLETED', 'FAILED')),
    CONSTRAINT chk_payments_outcome CHECK (
        (status = 'PENDING' AND provider_reference IS NULL AND failure_reason IS NULL)
        OR (status = 'COMPLETED' AND provider_reference IS NOT NULL AND failure_reason IS NULL)
        OR (status = 'FAILED' AND provider_reference IS NULL AND failure_reason IS NOT NULL)
    )
);

CREATE INDEX idx_payments_status
    ON payment_service.payments (status);
