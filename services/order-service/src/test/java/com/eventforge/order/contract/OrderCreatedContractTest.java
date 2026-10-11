package com.eventforge.order.contract;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.avro.AvroTypeException;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import com.eventforge.contracts.order.OrderCreated;
import com.eventforge.contracts.order.OrderCreatedPayload;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

/**
 * Verifies the OrderCreated Avro contract without Docker. The mock:// registry
 * keeps schema registration in memory; the real Schema Registry (including
 * compatibility enforcement) is covered by the Testcontainers suite.
 */
class OrderCreatedContractTest {

    private static final String TOPIC = "orders";
    private static final String REGISTRY_URL = "mock://order-created-contract";

    @Test
    void schemaDeclaresEnvelopeFieldsInContractOrder() {
        List<String> fields = OrderCreated.getClassSchema().getFields().stream()
                .map(Schema.Field::name)
                .toList();

        assertThat(fields).containsExactly(
                "eventId", "eventType", "eventVersion", "occurredAt", "aggregateType", "aggregateId", "payload");
    }

    @Test
    void totalAmountUsesDecimalPrecisionMatchingOrdersTable() {
        Schema totalAmount = OrderCreated.getClassSchema().getField("payload").schema()
                .getField("totalAmount").schema();

        LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) totalAmount.getLogicalType();
        assertThat(decimal.getPrecision()).isEqualTo(12);
        assertThat(decimal.getScale()).isEqualTo(2);
    }

    @Test
    void serializedEventRoundTripsThroughSchemaRegistryWithoutLosingData() {
        OrderCreated event = orderCreated(new BigDecimal("149.99"));

        byte[] bytes = serializer().serialize(TOPIC, event);
        OrderCreated decoded = (OrderCreated) deserializer().deserialize(TOPIC, bytes);

        assertThat(decoded).isEqualTo(event);
        assertThat(decoded.getOccurredAt()).isEqualTo(Instant.parse("2026-10-09T10:00:00.123Z"));
        assertThat(decoded.getPayload().getTotalAmount()).isEqualByComparingTo("149.99");
        assertThat(decoded.getPayload().getTotalAmount().scale()).isEqualTo(2);
    }

    @Test
    void rejectsTotalAmountWithMoreDecimalPlacesThanTheContract() {
        OrderCreated event = orderCreated(new BigDecimal("1.001"));

        assertThatThrownBy(() -> serializer().serialize(TOPIC, event))
                .hasRootCauseInstanceOf(AvroTypeException.class)
                .hasStackTraceContaining("scale");
    }

    private static KafkaAvroSerializer serializer() {
        KafkaAvroSerializer serializer = new KafkaAvroSerializer();
        serializer.configure(Map.of("schema.registry.url", REGISTRY_URL), false);
        return serializer;
    }

    private static KafkaAvroDeserializer deserializer() {
        KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
        deserializer.configure(
                Map.of("schema.registry.url", REGISTRY_URL, "specific.avro.reader", true), false);
        return deserializer;
    }

    private static OrderCreated orderCreated(BigDecimal totalAmount) {
        UUID orderId = UUID.randomUUID();
        return OrderCreated.newBuilder()
                .setEventId(UUID.randomUUID())
                .setEventType("OrderCreated")
                .setEventVersion(1)
                .setOccurredAt(Instant.parse("2026-10-09T10:00:00.123Z"))
                .setAggregateType("Order")
                .setAggregateId(orderId)
                .setPayload(OrderCreatedPayload.newBuilder()
                        .setOrderId(orderId)
                        .setCustomerId(UUID.randomUUID())
                        .setTotalAmount(totalAmount)
                        .setCurrency("USD")
                        .build())
                .build();
    }
}
