package com.eventforge.notification.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.UUID;

import org.junit.jupiter.api.Test;

class NotificationTest {

    @Test
    void createsNotificationInPendingStatus() {
        UUID eventId = UUID.randomUUID();

        Notification notification = Notification.create(eventId, "OrderCreated", "customer-1");

        assertThat(notification.getEventId()).isEqualTo(eventId);
        assertThat(notification.getType()).isEqualTo("OrderCreated");
        assertThat(notification.getRecipient()).isEqualTo("customer-1");
        assertThat(notification.getStatus()).isEqualTo(NotificationStatus.PENDING);
    }

    @Test
    void transitioningToSentOrFailedRequiresPendingStatus() {
        Notification notification = Notification.create(UUID.randomUUID(), "PaymentFailed", "order-1");

        notification.sent();
        assertThatThrownBy(notification::fail)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void rejectsBlankTypeAndRecipient() {
        assertThatThrownBy(() -> Notification.create(UUID.randomUUID(), " ", "recipient"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Notification.create(UUID.randomUUID(), "OrderCreated", " "))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Notification.create(null, "OrderCreated", "recipient"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
