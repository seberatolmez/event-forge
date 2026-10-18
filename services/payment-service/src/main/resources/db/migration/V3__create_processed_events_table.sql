CREATE TABLE payment_service.processed_events
(
    event_id      UUID PRIMARY KEY,
    event_type    VARCHAR(100) NOT NULL,
    event_version INTEGER NOT NULL CHECK (event_version > 0),
    received_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
