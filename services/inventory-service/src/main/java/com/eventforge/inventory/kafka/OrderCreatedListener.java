package com.eventforge.inventory.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
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
        if (!"OrderCreated".contentEquals(orderCreated.getEventType())
                || (orderCreated.getEventVersion() != 1 && orderCreated.getEventVersion() != 2)) {
            throw new IllegalArgumentException("unsupported OrderCreated event contract version or type");
        }

        inventoryProcessor.processOrderCreated(orderCreated);
        acknowledgment.acknowledge();
    }
}
