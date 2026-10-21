package com.eventforge.inventory.dedup;

import java.time.Instant;
import java.util.UUID;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "processed_events", schema = "inventory_service")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ProcessedEvent {

    @Id
    @Column(nullable = false, updatable = false)
    private UUID eventId;

    @Column(nullable = false, length = 100, updatable = false)
    private String eventType;

    @Column(name = "event_version", nullable = false, updatable = false)
    private int eventVersion;

    @CreationTimestamp
    @Column(name = "received_at", nullable = false, updatable = false)
    private Instant receivedAt;

    private ProcessedEvent(UUID eventId, String eventType, int eventVersion) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.eventVersion = eventVersion;
    }

    public static ProcessedEvent create(UUID eventId, String eventType, int eventVersion) {
        if (eventId == null || eventType == null || eventType.isBlank() || eventVersion < 1) {
            throw new IllegalArgumentException("valid event metadata is required");
        }
        return new ProcessedEvent(eventId, eventType, eventVersion);
    }
}
