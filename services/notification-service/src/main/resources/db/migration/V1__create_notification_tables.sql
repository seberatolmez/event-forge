CREATE SCHEMA IF NOT EXISTS notification_service;

CREATE TABLE notification_service.notifications
(
    id         UUID PRIMARY KEY,
    event_id   UUID NOT NULL UNIQUE,
    type       VARCHAR(100) NOT NULL,
    recipient  VARCHAR(255) NOT NULL,
    status     VARCHAR(16) NOT NULL CHECK (status IN ('PENDING', 'SENT', 'FAILED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_notifications_created_at
    ON notification_service.notifications (created_at);
