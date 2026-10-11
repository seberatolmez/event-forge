package com.eventforge.order.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

import com.eventforge.contracts.order.OrderCreated;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

@ExtendWith(MockitoExtension.class)
class OutboxPublisherTest {

    private static final int BATCH_SIZE = 10;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Mock
    private OutboxEventRepository outboxEventRepository;

    @Mock
    private KafkaTemplate<String, OrderCreated> kafkaTemplate;

    private SimpleMeterRegistry meterRegistry;
    private OutboxPublisher publisher;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        publisher = new OutboxPublisher(
                outboxEventRepository,
                kafkaTemplate,
                meterRegistry,
                BATCH_SIZE,
                1000,
                "orders");
    }

    @Test
    void publishesAvroEnvelopeWithAggregateKeyAndEventHeadersAfterKafkaAcknowledges() {
        UUID aggregateId = UUID.randomUUID();
        OutboxEvent event = outboxEvent(aggregateId);
        when(outboxEventRepository.findUnpublishedForUpdate(BATCH_SIZE)).thenReturn(List.of(event));
        CompletableFuture<SendResult<String, OrderCreated>> acknowledged = CompletableFuture.completedFuture(null);
        AtomicReference<ProducerRecord<String, OrderCreated>> sentRecord = new AtomicReference<>();
        when(kafkaTemplate.send(ArgumentMatchers.<ProducerRecord<String, OrderCreated>>any()))
                .thenAnswer(invocation -> {
                    sentRecord.set(invocation.getArgument(0));
                    return acknowledged;
                });

        publisher.publishPendingEvents();

        verify(kafkaTemplate).send(ArgumentMatchers.<ProducerRecord<String, OrderCreated>>any());
        ProducerRecord<String, OrderCreated> record = sentRecord.get();
        assertThat(record.topic()).isEqualTo("orders");
        assertThat(record.key()).isEqualTo(aggregateId.toString());
        assertThat(record.value().getEventId()).isEqualTo(event.getId());
        assertThat(record.value().getEventType().toString()).isEqualTo("OrderCreated");
        assertThat(record.value().getEventVersion()).isEqualTo(1);
        assertThat(record.value().getAggregateId()).isEqualTo(aggregateId);
        assertThat(record.value().getOccurredAt()).isEqualTo(Instant.parse("2026-10-08T12:00:00Z"));
        assertThat(record.value().getPayload().getOrderId()).isEqualTo(aggregateId);
        assertThat(record.value().getPayload().getCustomerId()).isNotNull();
        assertThat(record.value().getPayload().getTotalAmount()).isEqualByComparingTo("149.99");
        assertThat(record.value().getPayload().getCurrency().toString()).isEqualTo("USD");
        assertThat(headerValue(record, "eventId")).isEqualTo(event.getId().toString());
        assertThat(headerValue(record, "eventType")).isEqualTo("OrderCreated");
        assertThat(headerValue(record, "eventVersion")).isEqualTo("1");
        assertThat(event.getPublishedAt()).isNotNull();
        assertThat(meterRegistry.get("eventforge.outbox.events.published")
                .tag("event_type", "OrderCreated")
                .counter()
                .count()).isEqualTo(1.0);
    }

    @Test
    void leavesFailedEventPendingAndStopsTheBatchForRetry() {
        OutboxEvent failedEvent = outboxEvent(UUID.randomUUID());
        OutboxEvent laterEvent = outboxEvent(UUID.randomUUID());
        when(outboxEventRepository.findUnpublishedForUpdate(BATCH_SIZE))
                .thenReturn(List.of(failedEvent, laterEvent));
        CompletableFuture<SendResult<String, OrderCreated>> failed =
                CompletableFuture.failedFuture(new KafkaException("simulated broker failure"));
        when(kafkaTemplate.send(ArgumentMatchers.<ProducerRecord<String, OrderCreated>>any())).thenReturn(failed);

        publisher.publishPendingEvents();

        assertThat(failedEvent.getPublishedAt()).isNull();
        assertThat(failedEvent.getRetryCount()).isEqualTo(1);
        assertThat(laterEvent.getPublishedAt()).isNull();
        assertThat(laterEvent.getRetryCount()).isZero();
        assertThat(meterRegistry.get("eventforge.outbox.events.publish_failures")
                .tag("event_type", "OrderCreated")
                .counter()
                .count()).isEqualTo(1.0);
        verify(kafkaTemplate, times(1))
                .send(ArgumentMatchers.<ProducerRecord<String, OrderCreated>>any());
    }

    private OutboxEvent outboxEvent(UUID aggregateId) {
        OutboxEvent event = OutboxEvent.create(
                "Order",
                aggregateId,
                "OrderCreated",
                1,
                OBJECT_MAPPER.valueToTree(Map.of(
                        "orderId", aggregateId,
                        "customerId", UUID.randomUUID(),
                        "totalAmount", new BigDecimal("149.99"),
                        "currency", "USD")));
        ReflectionTestUtils.setField(event, "id", UUID.randomUUID());
        ReflectionTestUtils.setField(event, "createdAt", Instant.parse("2026-10-08T12:00:00Z"));
        return event;
    }

    private String headerValue(ProducerRecord<String, OrderCreated> record, String headerName) {
        Header header = record.headers().lastHeader(headerName);
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
