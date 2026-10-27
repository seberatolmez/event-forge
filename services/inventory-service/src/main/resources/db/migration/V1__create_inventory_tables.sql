CREATE SCHEMA IF NOT EXISTS inventory_service;

CREATE TABLE inventory_service.products
(
    id                 UUID PRIMARY KEY,
    sku                VARCHAR(100) NOT NULL UNIQUE,
    name               VARCHAR(255) NOT NULL,
    available_quantity INTEGER NOT NULL CHECK (available_quantity >= 0),
    reserved_quantity  INTEGER NOT NULL CHECK (reserved_quantity >= 0),
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE inventory_service.inventory_reservations
(
    id         UUID PRIMARY KEY,
    order_id   UUID NOT NULL,
    product_id UUID NOT NULL REFERENCES inventory_service.products (id),
    quantity   INTEGER NOT NULL CHECK (quantity > 0),
    status     VARCHAR(16) NOT NULL CHECK (status IN ('RESERVED', 'FAILED', 'RELEASED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_inventory_reservations_order_product UNIQUE (order_id, product_id)
);

CREATE INDEX idx_inventory_reservations_order_id
    ON inventory_service.inventory_reservations (order_id);

CREATE TABLE inventory_service.processed_events
(
    event_id      UUID PRIMARY KEY,
    event_type    VARCHAR(100) NOT NULL,
    event_version INTEGER NOT NULL CHECK (event_version > 0),
    received_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE inventory_service.outbox_events
(
    id             UUID PRIMARY KEY,
    aggregate_type VARCHAR(100) NOT NULL,
    aggregate_id   UUID NOT NULL,
    event_type     VARCHAR(100) NOT NULL,
    event_version  INTEGER NOT NULL CHECK (event_version > 0),
    payload        JSONB NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ,
    retry_count    INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0)
);

CREATE INDEX idx_inventory_outbox_events_unpublished
    ON inventory_service.outbox_events (created_at, id)
    WHERE published_at IS NULL;
