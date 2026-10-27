package com.eventforge.inventory.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.eventforge.contracts.order.OrderCreated;
import com.eventforge.inventory.service.InventoryProcessor;

@Component
@ConditionalOnProperty(prefix = "eventforge.inventory.consumer", name = "enabled", havingValue = "true")
@RequiredArgsConstructor
@Slf4j
public class OrderCreatedListener {

    private final InventoryProcessor inventoryProcessor;

    /**
     * The ack must not move past an uncommitted offset: {@link InventoryProcessor#processOrderCreated} runs its DB
     * transaction to completion before returning, so {@code acknowledge()} only ever fires after a durable commit.
     */
    @KafkaListener(
            topics = "${eventforge.inventory.consumer.orders-topic}",
            groupId = "${eventforge.inventory.consumer.group-id}")
    public void consume(ConsumerRecord<String, Object> record, Acknowledgment acknowledgment) {
        Object event = record.value();
        if (!(event instanceof OrderCreated orderCreated)) {
            log.debug(
                    "Ignoring non-OrderCreated record of type {}",
                    event == null ? "null" : event.getClass().getName());
            acknowledgment.acknowledge();
            return;
        }

        try {
            process(orderCreated);
        } catch (DuplicateKeyException e) {
            log.info("OrderCreated event {} already processed, skipping", orderCreated.getEventId());
        } catch (IllegalArgumentException e) {
            // Retrying a deterministically invalid event can never succeed and would block its partition forever.
            log.error("Discarding invalid OrderCreated event {}: {}", orderCreated.getEventId(), e.getMessage());
        }

        acknowledgment.acknowledge();
    }

    private void process(OrderCreated orderCreated) {
        if (!"OrderCreated".contentEquals(orderCreated.getEventType())
                || (orderCreated.getEventVersion() != 1 && orderCreated.getEventVersion() != 2)) {
            throw new IllegalArgumentException("unsupported OrderCreated event contract version or type");
        }
        inventoryProcessor.processOrderCreated(orderCreated);
    }
}
