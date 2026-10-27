package com.eventforge.notification.domain;

import java.time.Instant;
import java.util.UUID;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UuidGenerator;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "notifications", schema = "notification_service")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Notification {

    private static final int MAX_RECIPIENT_LENGTH = 255;

    @Id
    @UuidGenerator
    @Column(nullable = false, updatable = false)
    private UUID id;

    /**
     * Identifies the consumed business event; its UNIQUE constraint is the idempotency gate,
     * so a second delivery of the same event can never create a second notification.
     */
    @Column(name = "event_id", nullable = false, updatable = false)
    private UUID eventId;

    @Column(nullable = false, length = 100, updatable = false)
    private String type;

    @Column(nullable = false, updatable = false, length = MAX_RECIPIENT_LENGTH)
    private String recipient;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private NotificationStatus status;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    private Notification(UUID eventId, String type, String recipient) {
        this.eventId = eventId;
        this.type = type;
        this.recipient = recipient;
        this.status = NotificationStatus.PENDING;
    }

    public static Notification create(UUID eventId, String type, String recipient) {
        if (eventId == null) {
            throw new IllegalArgumentException("eventId must not be null");
        }
        if (type == null || type.isBlank() || type.length() > 100) {
            throw new IllegalArgumentException("type must not be blank or longer than 100 characters");
        }
        if (recipient == null || recipient.isBlank() || recipient.length() > MAX_RECIPIENT_LENGTH) {
            throw new IllegalArgumentException("recipient must not be blank or longer than 255 characters");
        }
        return new Notification(eventId, type, recipient);
    }

    public void sent() {
        ensurePending();
        this.status = NotificationStatus.SENT;
    }

    public void fail() {
        ensurePending();
        this.status = NotificationStatus.FAILED;
    }

    private void ensurePending() {
        if (status != NotificationStatus.PENDING) {
            throw new IllegalStateException(
                    "notification in status %s cannot transition to a final outcome".formatted(status));
        }
    }
}
