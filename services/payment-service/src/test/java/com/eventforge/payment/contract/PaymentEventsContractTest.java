package com.eventforge.payment.contract;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.eventforge.contracts.payment.PaymentCompleted;
import com.eventforge.contracts.payment.PaymentCompletedPayload;
import com.eventforge.contracts.payment.PaymentFailed;
import com.eventforge.contracts.payment.PaymentFailedPayload;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;

class PaymentEventsContractTest {

    private static final String TOPIC = "payments";
    private static final String REGISTRY_URL = "mock://payment-events-contract";

    @AfterEach
    void clearMockSchemaRegistry() {
        MockSchemaRegistry.dropScope("payment-events-contract");
    }

    @Test
    void paymentEventsUseTheCommonEnvelopeFieldOrder() {
        assertThat(PaymentCompleted.getClassSchema().getFields().stream()
                .map(Schema.Field::name)
                .toList())
                .containsExactly(
                        "eventId", "eventType", "eventVersion", "occurredAt", "aggregateType", "aggregateId", "payload");
        assertThat(PaymentFailed.getClassSchema().getFields().stream()
                .map(Schema.Field::name)
                .toList())
                .containsExactly(
                        "eventId", "eventType", "eventVersion", "occurredAt", "aggregateType", "aggregateId", "payload");
    }

    @Test
    void completedAmountUsesThePaymentStoragePrecisionAndScale() {
        Schema amountSchema = PaymentCompleted.getClassSchema().getField("payload").schema()
                .getField("amount").schema();
        LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) amountSchema.getLogicalType();

        assertThat(decimal.getPrecision()).isEqualTo(12);
        assertThat(decimal.getScale()).isEqualTo(2);
    }

    @Test
    void bothEventTypesRoundTripOnThePaymentsTopicWithRecordNameSubjects() throws Exception {
        PaymentCompleted completed = completedEvent();
        PaymentFailed failed = failedEvent();
        KafkaAvroSerializer serializer = serializer();
        KafkaAvroDeserializer deserializer = deserializer();

        PaymentCompleted decodedCompleted = (PaymentCompleted) deserializer.deserialize(
                TOPIC, serializer.serialize(TOPIC, completed));
        PaymentFailed decodedFailed = (PaymentFailed) deserializer.deserialize(
                TOPIC, serializer.serialize(TOPIC, failed));

        assertThat(decodedCompleted).isEqualTo(completed);
        assertThat(decodedFailed).isEqualTo(failed);
        assertThat(MockSchemaRegistry.getClientForScope("payment-events-contract").getAllSubjects())
                .containsExactlyInAnyOrder(
                        "payments-com.eventforge.contracts.payment.PaymentCompleted",
                        "payments-com.eventforge.contracts.payment.PaymentFailed");
    }

    private static KafkaAvroSerializer serializer() {
        KafkaAvroSerializer serializer = new KafkaAvroSerializer();
        serializer.configure(Map.of(
                "schema.registry.url", REGISTRY_URL,
                "value.subject.name.strategy", TopicRecordNameStrategy.class.getName()), false);
        return serializer;
    }

    private static KafkaAvroDeserializer deserializer() {
        KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
        deserializer.configure(Map.of(
                "schema.registry.url", REGISTRY_URL,
                "specific.avro.reader", true), false);
        return deserializer;
    }

    private static PaymentCompleted completedEvent() {
        UUID paymentId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        return PaymentCompleted.newBuilder()
                .setEventId(UUID.randomUUID())
                .setEventType("PaymentCompleted")
                .setEventVersion(1)
                .setOccurredAt(Instant.parse("2026-10-12T10:00:00.123Z"))
                .setAggregateType("Payment")
                .setAggregateId(paymentId)
                .setPayload(PaymentCompletedPayload.newBuilder()
                        .setPaymentId(paymentId)
                        .setOrderId(orderId)
                        .setAmount(new BigDecimal("149.99"))
                        .setCurrency("USD")
                        .setProviderReference("simulated-payment-123")
                        .build())
                .build();
    }

    private static PaymentFailed failedEvent() {
        UUID paymentId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        return PaymentFailed.newBuilder()
                .setEventId(UUID.randomUUID())
                .setEventType("PaymentFailed")
                .setEventVersion(1)
                .setOccurredAt(Instant.parse("2026-10-12T10:00:01.123Z"))
                .setAggregateType("Payment")
                .setAggregateId(paymentId)
                .setPayload(PaymentFailedPayload.newBuilder()
                        .setPaymentId(paymentId)
                        .setOrderId(orderId)
                        .setReason("amount exceeds simulated provider limit")
                        .build())
                .build();
    }
}
