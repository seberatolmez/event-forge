package com.eventforge.payment.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.eventforge.contracts.order.OrderCreated;
import com.eventforge.payment.service.PaymentProcessor;

@Component
@ConditionalOnProperty(prefix = "eventforge.payment.consumer", name = "enabled", havingValue = "true")
@RequiredArgsConstructor
@Slf4j
public class OrderCreatedListener {

    private final PaymentProcessor paymentProcessor;

    @KafkaListener(
            topics = "${eventforge.payment.consumer.orders-topic}",
            groupId = "${eventforge.payment.consumer.group-id}")
    public void consume(ConsumerRecord<String, Object> record, Acknowledgment acknowledgment) {
        Object event = record.value();
        if (!(event instanceof OrderCreated orderCreated)) {
            log.debug(
                    "Ignoring non-OrderCreated record of type {}",
                    event == null ? "null" : event.getClass().getName());
            acknowledgment.acknowledge();
            return;
        }
        if (!"OrderCreated".contentEquals(orderCreated.getEventType()) || orderCreated.getEventVersion() != 1) {
            throw new IllegalArgumentException("unsupported OrderCreated event contract version or type");
        }

        paymentProcessor.processOrderCreated(orderCreated);
        acknowledgment.acknowledge();
    }
}
