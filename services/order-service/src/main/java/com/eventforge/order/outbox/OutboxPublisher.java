package com.eventforge.order.outbox;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import com.eventforge.contracts.order.OrderCreated;
import com.eventforge.contracts.order.OrderCreatedPayload;
import com.fasterxml.jackson.databind.JsonNode;

@Component
@ConditionalOnProperty(prefix = "eventforge.outbox.publisher", name = "enabled", havingValue = "true", matchIfMissing = true)
public class OutboxPublisher {

    private static final Logger logger = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, OrderCreated> kafkaTemplate;
    private final MeterRegistry meterRegistry;
    private final int batchSize;
    private final long sendTimeoutMs;
    private final String ordersTopic;

    public OutboxPublisher(
            OutboxEventRepository outboxEventRepository,
            KafkaTemplate<String, OrderCreated> kafkaTemplate,
            MeterRegistry meterRegistry,
            @Value("${eventforge.outbox.publisher.batch-size:100}") int batchSize,
            @Value("${eventforge.outbox.publisher.send-timeout-ms:10000}") long sendTimeoutMs,
            @Value("${eventforge.outbox.publisher.orders-topic:orders}") String ordersTopic) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.meterRegistry = meterRegistry;
        this.batchSize = batchSize;
        this.sendTimeoutMs = sendTimeoutMs;
        this.ordersTopic = ordersTopic;
    }

    @Scheduled(fixedDelayString = "${eventforge.outbox.publisher.poll-interval-ms:1000}")
    @Transactional
    public void publishPendingEvents() {
        List<OutboxEvent> events = outboxEventRepository.findUnpublishedForUpdate(batchSize);
        for (OutboxEvent event : events) {
            if (!publish(event)) {
                break;
            }
        }
    }

    private boolean publish(OutboxEvent event) {
        long startedAt = System.nanoTime();
        try {
            ProducerRecord<String, OrderCreated> record = toRecord(event);
            kafkaTemplate.send(record).get(sendTimeoutMs, TimeUnit.MILLISECONDS);
            event.markPublished(Instant.now());
            Counter.builder("eventforge.outbox.events.published")
                    .description("Outbox events acknowledged by Kafka")
                    .tag("event_type", event.getEventType())
                    .register(meterRegistry)
                    .increment();
            return true;
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            recordFailure(event, exception);
            return false;
        } catch (Exception exception) {
            recordFailure(event, exception);
            return false;
        } finally {
            Timer.builder("eventforge.outbox.publish.duration")
                    .description("Time spent publishing an outbox event")
                    .tag("event_type", event.getEventType())
                    .register(meterRegistry)
                    .record(System.nanoTime() - startedAt, TimeUnit.NANOSECONDS);
        }
    }

    private ProducerRecord<String, OrderCreated> toRecord(OutboxEvent event) {
        JsonNode payload = event.getPayload();
        UUID orderId = UUID.fromString(payload.path("orderId").asText());
        UUID customerId = UUID.fromString(payload.path("customerId").asText());
        BigDecimal totalAmount = payload.path("totalAmount").decimalValue();
        String currency = payload.path("currency").asText();

        OrderCreated avroEvent = OrderCreated.newBuilder()
                .setEventId(event.getId())
                .setEventType(event.getEventType())
                .setEventVersion(event.getEventVersion())
                .setOccurredAt(event.getCreatedAt().truncatedTo(ChronoUnit.MILLIS))
                .setAggregateType(event.getAggregateType())
                .setAggregateId(event.getAggregateId())
                .setPayload(OrderCreatedPayload.newBuilder()
                        .setOrderId(orderId)
                        .setCustomerId(customerId)
                        .setTotalAmount(totalAmount)
                        .setCurrency(currency)
                        .build())
                .build();

        ProducerRecord<String, OrderCreated> record = new ProducerRecord<>(
                ordersTopic,
                event.getAggregateId().toString(),
                avroEvent);
        record.headers()
                .add("eventId", headerValue(event.getId().toString()))
                .add("eventType", headerValue(event.getEventType()))
                .add("eventVersion", headerValue(Integer.toString(event.getEventVersion())))
                .add("aggregateType", headerValue(event.getAggregateType()))
                .add("aggregateId", headerValue(event.getAggregateId().toString()))
                .add("occurredAt", headerValue(event.getCreatedAt().toString()));
        return record;
    }

    private void recordFailure(OutboxEvent event, Exception exception) {
        event.incrementRetryCount();
        Counter.builder("eventforge.outbox.events.publish_failures")
                .description("Outbox event publish attempts that failed")
                .tag("event_type", event.getEventType())
                .register(meterRegistry)
                .increment();
        logger.warn("Failed to publish outbox event {}; it will be retried", event.getId(), exception);
    }

    private byte[] headerValue(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
