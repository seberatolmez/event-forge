package com.eventforge.notification.service;

import java.util.UUID;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.eventforge.contracts.inventory.InventoryReservationFailed;
import com.eventforge.contracts.order.OrderCreated;
import com.eventforge.contracts.payment.PaymentCompleted;
import com.eventforge.contracts.payment.PaymentFailed;
import com.eventforge.notification.domain.Notification;
import com.eventforge.notification.provider.MockNotificationProvider;
import com.eventforge.notification.repository.NotificationRepository;

@Service
@RequiredArgsConstructor
@Transactional
@Slf4j
public class NotificationProcessor {

    private final NotificationRepository notificationRepository;
    private final MockNotificationProvider mockNotificationProvider;

    public void processOrderCreated(OrderCreated event) {
        create(event.getEventId(), event.getEventType().toString(),
                event.getPayload().getCustomerId().toString());
    }

    public void processPaymentCompleted(PaymentCompleted event) {
        create(event.getEventId(), event.getEventType().toString(),
                event.getPayload().getOrderId().toString());
    }

    public void processPaymentFailed(PaymentFailed event) {
        create(event.getEventId(), event.getEventType().toString(),
                event.getPayload().getOrderId().toString());
    }

    public void processInventoryReservationFailed(InventoryReservationFailed event) {
        create(event.getEventId(), event.getEventType().toString(),
                event.getPayload().getOrderId().toString());
    }

    /**
     * The UNIQUE(event_id) constraint is the idempotency gate: a re-delivered event collides on INSERT,
     * and the listener discards the resulting DuplicateKeyException instead of re-creating a notification.
     */
    private void create(UUID eventId, String type, String recipient) {
        Notification notification = notificationRepository.saveAndFlush(
                Notification.create(eventId, type, recipient));
        mockNotificationProvider.send(notification);
        notification.sent();
        log.info("Notification {} of type {} sent to {}", notification.getId(), type, recipient);
    }
}
