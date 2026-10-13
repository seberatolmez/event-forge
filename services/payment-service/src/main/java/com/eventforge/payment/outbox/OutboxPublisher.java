package com.eventforge.payment.outbox;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.avro.specific.SpecificRecord;
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

import com.eventforge.contracts.payment.PaymentCompleted;
import com.eventforge.contracts.payment.PaymentCompletedPayload;
import com.eventforge.contracts.payment.PaymentFailed;
import com.eventforge.contracts.payment.PaymentFailedPayload;
import com.fasterxml.jackson.databind.JsonNode;

@Component
@ConditionalOnProperty(prefix = "eventforge.outbox.publisher", name = "enabled", havingValue = "true")
public class OutboxPublisher {

    private static final Logger logger = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, SpecificRecord> kafkaTemplate;
    private final MeterRegistry meterRegistry;
    private final int batchSize;
    private final long sendTimeoutMs;
    private final String paymentsTopic;

    public OutboxPublisher(
            OutboxEventRepository outboxEventRepository,
            KafkaTemplate<String, SpecificRecord> kafkaTemplate,
            MeterRegistry meterRegistry,
            @Value("${eventforge.outbox.publisher.batch-size}") int batchSize,
            @Value("${eventforge.outbox.publisher.send-timeout-ms}") long sendTimeoutMs,
            @Value("${eventforge.outbox.publisher.payments-topic}") String paymentsTopic) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.meterRegistry = meterRegistry;
        this.batchSize = batchSize;
        this.sendTimeoutMs = sendTimeoutMs;
        this.paymentsTopic = paymentsTopic;
    }

    @Scheduled(fixedDelayString = "${eventforge.outbox.publisher.poll-interval-ms}")
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
            ProducerRecord<String, SpecificRecord> record = toRecord(event);
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

    private ProducerRecord<String, SpecificRecord> toRecord(OutboxEvent event) {
        JsonNode payload = event.getPayload();
        UUID paymentId = UUID.fromString(payload.path("paymentId").asText());
        UUID orderId = UUID.fromString(payload.path("orderId").asText());
        SpecificRecord avroEvent = switch (event.getEventType()) {
            case "PaymentCompleted" -> PaymentCompleted.newBuilder()
                    .setEventId(event.getId())
                    .setEventType(event.getEventType())
                    .setEventVersion(event.getEventVersion())
                    .setOccurredAt(event.getCreatedAt().truncatedTo(ChronoUnit.MILLIS))
                    .setAggregateType(event.getAggregateType())
                    .setAggregateId(event.getAggregateId())
                    .setPayload(PaymentCompletedPayload.newBuilder()
                            .setPaymentId(paymentId)
                            .setOrderId(orderId)
                            .setAmount(payload.path("amount").decimalValue())
                            .setCurrency(payload.path("currency").asText())
                            .setProviderReference(payload.path("providerReference").asText())
                            .build())
                    .build();
            case "PaymentFailed" -> PaymentFailed.newBuilder()
                    .setEventId(event.getId())
                    .setEventType(event.getEventType())
                    .setEventVersion(event.getEventVersion())
                    .setOccurredAt(event.getCreatedAt().truncatedTo(ChronoUnit.MILLIS))
                    .setAggregateType(event.getAggregateType())
                    .setAggregateId(event.getAggregateId())
                    .setPayload(PaymentFailedPayload.newBuilder()
                            .setPaymentId(paymentId)
                            .setOrderId(orderId)
                            .setReason(payload.path("reason").asText())
                            .build())
                    .build();
            default -> throw new IllegalArgumentException(
                    "unsupported payment event type: %s".formatted(event.getEventType()));
        };

        ProducerRecord<String, SpecificRecord> record = new ProducerRecord<>(
                paymentsTopic,
                orderId.toString(),
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
