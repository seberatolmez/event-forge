package com.eventforge.order.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.eventforge.order.outbox.OutboxEvent;
import com.eventforge.order.outbox.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.micrometer.core.instrument.MeterRegistry;

@SpringBootTest(properties = {
        "eventforge.outbox.publisher.enabled=true",
        "eventforge.outbox.publisher.poll-interval-ms=100",
        "eventforge.outbox.publisher.send-timeout-ms=1000"
})
@Testcontainers(disabledWithoutDocker = true)
class OutboxPublisherFailureIntegrationTest {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired
    OutboxEventRepository outboxEventRepository;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    MeterRegistry meterRegistry;

    @MockitoBean
    KafkaTemplate<String, String> kafkaTemplate;

    @Test
    void failedKafkaSendLeavesEventPendingAndRecordsRetryAndMetric() throws Exception {
        when(kafkaTemplate.send(ArgumentMatchers.<ProducerRecord<String, String>>any()))
                .thenReturn(CompletableFuture.failedFuture(new KafkaException("simulated broker failure")));

        OutboxEvent savedEvent = outboxEventRepository.saveAndFlush(OutboxEvent.create(
                "Order",
                UUID.randomUUID(),
                "OrderCreated",
                1,
                objectMapper.readTree("{\"orderId\":\"test\"}")));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            OutboxEvent storedEvent = outboxEventRepository.findById(savedEvent.getId()).orElseThrow();
            assertThat(storedEvent.getPublishedAt()).isNull();
            assertThat(storedEvent.getRetryCount()).isGreaterThanOrEqualTo(1);
            assertThat(meterRegistry.get("eventforge.outbox.events.publish_failures")
                    .tag("event_type", "OrderCreated")
                    .counter()
                    .count()).isGreaterThanOrEqualTo(1.0);
        });
    }
}
