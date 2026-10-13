package com.eventforge.payment.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import com.eventforge.contracts.payment.PaymentCompleted;
import com.eventforge.contracts.payment.PaymentFailed;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

class PaymentOutboxPublisherTest {

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    void publishesBothPaymentEventTypesUsingOrderKeysAndMarksOutboxRowsPublished() {
        OutboxEventRepository repository = mock(OutboxEventRepository.class);
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, SpecificRecord> kafkaTemplate = mock(KafkaTemplate.class);
        ObjectMapper objectMapper = new ObjectMapper();

        UUID completedPaymentId = UUID.randomUUID();
        UUID completedOrderId = UUID.randomUUID();
        UUID completedEventId = UUID.randomUUID();
        OutboxEvent completed = OutboxEvent.create(
                "Payment",
                completedPaymentId,
                "PaymentCompleted",
                1,
                objectMapper.valueToTree(Map.of(
                        "paymentId", completedPaymentId,
                        "orderId", completedOrderId,
                        "amount", new BigDecimal("149.99"),
                        "currency", "USD",
                        "providerReference", "simulated-payment-123")));
        setPersistedMetadata(completed, completedEventId);

        UUID failedPaymentId = UUID.randomUUID();
        UUID failedOrderId = UUID.randomUUID();
        OutboxEvent failed = OutboxEvent.create(
                "Payment",
                failedPaymentId,
                "PaymentFailed",
                1,
                objectMapper.valueToTree(Map.of(
                        "paymentId", failedPaymentId,
                        "orderId", failedOrderId,
                        "reason", "amount exceeds simulated provider limit")));
        setPersistedMetadata(failed, UUID.randomUUID());

        when(repository.findUnpublishedForUpdate(10)).thenReturn(List.of(completed, failed));
        doReturn(CompletableFuture.completedFuture(null))
                .when(kafkaTemplate)
                .send(any(ProducerRecord.class));

        OutboxPublisher publisher = new OutboxPublisher(
                repository,
                kafkaTemplate,
                new SimpleMeterRegistry(),
                10,
                1000,
                "payments");

        publisher.publishPendingEvents();

        ArgumentCaptor<ProducerRecord> records = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate, times(2)).send(records.capture());
        List<ProducerRecord> published = records.getAllValues();
        assertThat(published).hasSize(2);
        assertThat(published.get(0).topic()).isEqualTo("payments");
        assertThat(published.get(0).key()).isEqualTo(completedOrderId.toString());
        assertThat(published.get(0).value()).isInstanceOf(PaymentCompleted.class);
        PaymentCompleted completedEvent = (PaymentCompleted) published.get(0).value();
        assertThat(completedEvent.getEventId()).isEqualTo(completedEventId);
        assertThat(completedEvent.getPayload().getAmount()).isEqualByComparingTo("149.99");
        assertThat(published.get(1).key()).isEqualTo(failedOrderId.toString());
        assertThat(published.get(1).value()).isInstanceOf(PaymentFailed.class);
        assertThat(completed.getPublishedAt()).isNotNull();
        assertThat(failed.getPublishedAt()).isNotNull();
    }

    private void setPersistedMetadata(OutboxEvent event, UUID eventId) {
        ReflectionTestUtils.setField(event, "id", eventId);
        ReflectionTestUtils.setField(event, "createdAt", Instant.parse("2026-10-12T10:00:00.123Z"));
    }
}
