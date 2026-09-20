CREATE SCHEMA IF NOT EXISTS order_service;

CREATE TABLE IF NOT EXISTS order_service.orders
(
    id           UUID PRIMARY KEY,
    customer_id  UUID NOT NULL,
    total_amount NUMERIC(12, 2) NOT NULL CHECK (total_amount > 0),
    currency     VARCHAR(3) NOT NULL,
    status       VARCHAR(32) NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_orders_status
        CHECK (status IN ('PENDING',
                          'PAYMENT_COMPLETED',
                          'PAYMENT_FAILED',
                          'INVENTORY_RESERVED',
                          'INVENTORY_FAILED',
                          'COMPLETED',
                          'CANCELLED'))
);

CREATE INDEX IF NOT EXISTS idx_orders_customer_id
    ON order_service.orders (customer_id);

CREATE INDEX IF NOT EXISTS idx_orders_status
    ON order_service.orders (status);
