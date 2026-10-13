package com.eventforge.payment.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.eventforge.contracts.order.OrderCreated;
import com.eventforge.payment.service.PaymentProcessor;

@Component
@ConditionalOnProperty(prefix = "eventforge.payment.consumer", name = "enabled", havingValue = "true")
public class OrderCreatedListener {

    private static final Logger logger = LoggerFactory.getLogger(OrderCreatedListener.class);

    private final PaymentProcessor paymentProcessor;

    public OrderCreatedListener(PaymentProcessor paymentProcessor) {
        this.paymentProcessor = paymentProcessor;
    }

    @KafkaListener(
            topics = "${eventforge.payment.consumer.orders-topic}",
            groupId = "${eventforge.payment.consumer.group-id}")
    public void consume(ConsumerRecord<String, Object> record, Acknowledgment acknowledgment) {
        Object event = record.value();
        if (!(event instanceof OrderCreated orderCreated)) {
            logger.debug(
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
