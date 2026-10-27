package com.eventforge.notification.kafka;

import java.util.UUID;
import java.util.stream.IntStream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.eventforge.contracts.inventory.InventoryReservationFailed;
import com.eventforge.contracts.order.OrderCreated;
import com.eventforge.contracts.payment.PaymentCompleted;
import com.eventforge.contracts.payment.PaymentFailed;
import com.eventforge.notification.service.NotificationProcessor;

@Component
@ConditionalOnProperty(prefix = "eventforge.notification.consumer", name = "enabled", havingValue = "true")
@RequiredArgsConstructor
@Slf4j
public class BusinessEventListener {

    private final NotificationProcessor notificationProcessor;

    /**
     * The ack must not move past an uncommitted offset: every processor call runs its DB transaction to
     * completion before returning, so {@code acknowledge()} only ever fires after a durable outcome.
     */
    @KafkaListener(
            topics = {
                    "${eventforge.notification.consumer.orders-topic}",
                    "${eventforge.notification.consumer.payments-topic}",
                    "${eventforge.notification.consumer.inventory-topic}"},
            groupId = "${eventforge.notification.consumer.group-id}")
    public void consume(ConsumerRecord<String, Object> record, Acknowledgment acknowledgment) {
        Object event = record.value();
        if (event instanceof OrderCreated orderCreated) {
            requireContract("OrderCreated", orderCreated.getEventType(), orderCreated.getEventVersion(), 1, 2);
            run(orderCreated.getEventId(),
                    () -> notificationProcessor.processOrderCreated(orderCreated),
                    acknowledgment);
        } else if (event instanceof PaymentCompleted completed) {
            requireContract("PaymentCompleted", completed.getEventType(), completed.getEventVersion(), 1);
            run(completed.getEventId(),
                    () -> notificationProcessor.processPaymentCompleted(completed),
                    acknowledgment);
        } else if (event instanceof PaymentFailed failed) {
            requireContract("PaymentFailed", failed.getEventType(), failed.getEventVersion(), 1);
            run(failed.getEventId(),
                    () -> notificationProcessor.processPaymentFailed(failed),
                    acknowledgment);
        } else if (event instanceof InventoryReservationFailed failed) {
            requireContract("InventoryReservationFailed", failed.getEventType(), failed.getEventVersion(), 1);
            run(failed.getEventId(),
                    () -> notificationProcessor.processInventoryReservationFailed(failed),
                    acknowledgment);
        } else {
            log.debug(
                    "Ignoring unsupported event on topic {} of type {}",
                    record.topic(),
                    event == null ? "null" : event.getClass().getName());
            acknowledgment.acknowledge();
        }
    }

    private void requireContract(String expectedType, String actualType, int actualVersion, int... allowedVersions) {
        boolean typeMatches = expectedType.equals(actualType);
        boolean versionAllowed = IntStream.of(allowedVersions).anyMatch(v -> v == actualVersion);
        if (!typeMatches || !versionAllowed) {
            throw new IllegalArgumentException(
                    "unsupported %s event contract version or type".formatted(expectedType));
        }
    }

    private void run(UUID eventId, Runnable processing, Acknowledgment acknowledgment) {
        try {
            processing.run();
        } catch (DuplicateKeyException e) {
            log.info("Event {} already processed, notification already created", eventId);
        } catch (IllegalArgumentException e) {
            // Retrying a deterministically invalid event can never succeed and would block its partition forever.
            log.error("Discarding invalid event {}: {}", eventId, e.getMessage());
        }
        acknowledgment.acknowledge();
    }
}
