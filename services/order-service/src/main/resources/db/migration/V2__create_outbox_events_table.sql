CREATE TABLE order_service.outbox_events
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

CREATE INDEX idx_outbox_events_unpublished
    ON order_service.outbox_events (created_at, id)
    WHERE published_at IS NULL;
