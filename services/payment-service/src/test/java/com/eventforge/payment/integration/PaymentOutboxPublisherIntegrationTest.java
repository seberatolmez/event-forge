package com.eventforge.payment.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin.NewTopics;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.eventforge.contracts.order.OrderCreated;
import com.eventforge.contracts.order.OrderCreatedPayload;
import com.eventforge.contracts.payment.PaymentCompleted;
import com.eventforge.contracts.payment.PaymentFailed;
import com.eventforge.payment.dedup.ProcessedEventRepository;
import com.eventforge.payment.domain.Payment;
import com.eventforge.payment.domain.PaymentStatus;
import com.eventforge.payment.outbox.OutboxEvent;
import com.eventforge.payment.outbox.OutboxEventRepository;
import com.eventforge.payment.repository.PaymentRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

@SpringBootTest(properties = {
        "eventforge.outbox.publisher.enabled=true",
        "eventforge.outbox.publisher.poll-interval-ms=100",
        "eventforge.outbox.publisher.send-timeout-ms=5000",
        "eventforge.payment.consumer.group-id=payment-service",
        "eventforge.payment.consumer.retry-backoff-ms=100"
})
@Import(PaymentOutboxPublisherIntegrationTest.KafkaTopicConfiguration.class)
@Testcontainers(disabledWithoutDocker = true)
class PaymentOutboxPublisherIntegrationTest {

    private static final String PAYMENTS_TOPIC = "payments";
    private static final String PAYMENT_CONSUMER_GROUP = "payment-service";
    private static final Network KAFKA_NETWORK = Network.newNetwork();

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    static final ConfluentKafkaContainer kafka = new ConfluentKafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.7.1"))
                    .withNetwork(KAFKA_NETWORK)
                    .withListener("kafka:19092");

    @Container
    static final GenericContainer<?> schemaRegistry = new GenericContainer<>(
            DockerImageName.parse("confluentinc/cp-schema-registry:7.7.1"))
                    .withNetwork(KAFKA_NETWORK)
                    .withNetworkAliases("schema-registry")
                    .withExposedPorts(8081)
                    .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                    .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                    .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:19092")
                    .withEnv("SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR", "1")
                    .dependsOn(kafka)
                    .waitingFor(Wait.forHttp("/subjects").forPort(8081).forStatusCode(200))
                    .withStartupTimeout(Duration.ofMinutes(2));

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add(
                "spring.kafka.producer.properties[schema.registry.url]",
                PaymentOutboxPublisherIntegrationTest::schemaRegistryUrl);
        registry.add(
                "spring.kafka.consumer.properties[schema.registry.url]",
                PaymentOutboxPublisherIntegrationTest::schemaRegistryUrl);
    }

    @Autowired
    OutboxEventRepository outboxEventRepository;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    PaymentRepository paymentRepository;

    @Autowired
    ProcessedEventRepository processedEventRepository;

    @Autowired
    KafkaTemplate<String, SpecificRecord> kafkaTemplate;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Test
    void publishesCompletedAndFailedEventsWithPerRecordSubjectsAndOrderKeys() throws Exception {
        UUID completedPaymentId = UUID.randomUUID();
        UUID completedOrderId = UUID.randomUUID();
        UUID failedPaymentId = UUID.randomUUID();
        UUID failedOrderId = UUID.randomUUID();

        JsonNode completedPayload = objectMapper.valueToTree(Map.of(
                "paymentId", completedPaymentId,
                "orderId", completedOrderId,
                "amount", new BigDecimal("149.99"),
                "currency", "USD",
                "providerReference", "simulated-payment-123"));
        JsonNode failedPayload = objectMapper.valueToTree(Map.of(
                "paymentId", failedPaymentId,
                "orderId", failedOrderId,
                "reason", "amount exceeds simulated provider limit"));

        List<OutboxEvent> events = outboxEventRepository.saveAllAndFlush(List.of(
                OutboxEvent.create("Payment", completedPaymentId, "PaymentCompleted", 1, completedPayload),
                OutboxEvent.create("Payment", failedPaymentId, "PaymentFailed", 1, failedPayload)));

        List<ConsumerRecord<String, SpecificRecord>> records = consumePaymentEvents(
                Set.of(completedOrderId, failedOrderId));

        assertThat(records).hasSize(2);
        ConsumerRecord<String, SpecificRecord> completedRecord = records.stream()
                .filter(record -> record.value() instanceof PaymentCompleted)
                .findFirst()
                .orElseThrow();
        PaymentCompleted completed = (PaymentCompleted) completedRecord.value();
        assertThat(completedRecord.key()).isEqualTo(completedOrderId.toString());
        assertThat(completed.getEventType().toString()).isEqualTo("PaymentCompleted");
        assertThat(completed.getAggregateType().toString()).isEqualTo("Payment");
        assertThat(completed.getAggregateId()).isEqualTo(completedPaymentId);
        assertThat(completed.getPayload().getOrderId()).isEqualTo(completedOrderId);
        assertThat(completed.getPayload().getAmount()).isEqualByComparingTo("149.99");
        assertThat(headerValue(completedRecord, "eventId")).isEqualTo(completed.getEventId().toString());

        ConsumerRecord<String, SpecificRecord> failedRecord = records.stream()
                .filter(record -> record.value() instanceof PaymentFailed)
                .findFirst()
                .orElseThrow();
        PaymentFailed failed = (PaymentFailed) failedRecord.value();
        assertThat(failedRecord.key()).isEqualTo(failedOrderId.toString());
        assertThat(failed.getEventType().toString()).isEqualTo("PaymentFailed");
        assertThat(failed.getAggregateId()).isEqualTo(failedPaymentId);
        assertThat(failed.getPayload().getReason().toString())
                .isEqualTo("amount exceeds simulated provider limit");

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            List<OutboxEvent> persistedEvents = outboxEventRepository.findAllById(
                    events.stream().map(OutboxEvent::getId).toList());
            assertThat(persistedEvents).hasSize(2).allSatisfy(event -> {
                assertThat(event.getPublishedAt()).isNotNull();
                assertThat(event.getRetryCount()).isZero();
            });
        });

        try (SchemaRegistryClient registryClient = new CachedSchemaRegistryClient(schemaRegistryUrl(), 10)) {
            assertThat(registryClient.getAllSubjects()).contains(
                    "payments-com.eventforge.contracts.payment.PaymentCompleted",
                    "payments-com.eventforge.contracts.payment.PaymentFailed");
        }
    }

    @Test
    void consumesOrderCreatedPersistsOnePaymentAndPublishesTheMatchingOutcome() throws Exception {
        UUID completedOrderId = UUID.randomUUID();
        UUID failedOrderId = UUID.randomUUID();
        OrderCreated completedOrder = orderCreated(completedOrderId, "149.99");
        OrderCreated failedOrder = orderCreated(failedOrderId, "1500.00");

        long lastInputOffset = sendOrderCreated(completedOrder);
        lastInputOffset = Math.max(lastInputOffset, sendOrderCreated(completedOrder));
        lastInputOffset = Math.max(lastInputOffset, sendOrderCreated(failedOrder));

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            Payment completed = paymentRepository.findByOrderId(completedOrderId).orElseThrow();
            Payment failed = paymentRepository.findByOrderId(failedOrderId).orElseThrow();
            assertThat(completed.getStatus()).isEqualTo(PaymentStatus.COMPLETED);
            assertThat(completed.getProviderReference()).isEqualTo("simulated-payment-" + completedOrderId);
            assertThat(failed.getStatus()).isEqualTo(PaymentStatus.FAILED);
            assertThat(failed.getFailureReason()).isEqualTo("amount exceeds simulated provider limit");
            assertThat(outboxEventRepository.findAllByAggregateId(completed.getId())).hasSize(1);
            assertThat(outboxEventRepository.findAllByAggregateId(failed.getId())).hasSize(1);
        });
        awaitPaymentConsumerCommitted(lastInputOffset);

        List<ConsumerRecord<String, SpecificRecord>> records = consumePaymentEvents(
                Set.of(completedOrderId, failedOrderId));
        assertThat(records).hasSize(2);
        assertThat(records.stream().map(ConsumerRecord::key))
                .containsExactlyInAnyOrder(completedOrderId.toString(), failedOrderId.toString());
        assertThat(records).anySatisfy(record -> {
            assertThat(record.value()).isInstanceOf(PaymentCompleted.class);
            assertThat(((PaymentCompleted) record.value()).getPayload().getOrderId()).isEqualTo(completedOrderId);
        });
        assertThat(records).anySatisfy(record -> {
            assertThat(record.value()).isInstanceOf(PaymentFailed.class);
            assertThat(((PaymentFailed) record.value()).getPayload().getOrderId()).isEqualTo(failedOrderId);
        });

        try (SchemaRegistryClient registryClient = new CachedSchemaRegistryClient(schemaRegistryUrl(), 10)) {
            assertThat(registryClient.getAllSubjects()).contains(
                    "orders-com.eventforge.contracts.order.OrderCreated",
                    "payments-com.eventforge.contracts.payment.PaymentCompleted",
                    "payments-com.eventforge.contracts.payment.PaymentFailed");
        }
    }

    @Test
    void databaseFailureRollsBackPaymentAndOutboxThenRetriesWithoutCommittingTheOrderEvent() throws Exception {
        UUID orderId = UUID.randomUUID();
        long inputOffset;
        installOutboxInsertFailure();
        try {
            inputOffset = sendOrderCreated(orderCreated(orderId, "149.99"));

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                Boolean triggerWasCalled = jdbcTemplate.queryForObject(
                        "SELECT is_called FROM payment_service.payment_outbox_reject_attempts", Boolean.class);
                Long outboxRowsForOrder = jdbcTemplate.queryForObject(
                        "SELECT count(*) FROM payment_service.outbox_events WHERE payload->>'orderId' = ?",
                        Long.class,
                        orderId.toString());
                assertThat(triggerWasCalled).isTrue();
                assertThat(paymentRepository.findByOrderId(orderId)).isEmpty();
                assertThat(outboxRowsForOrder).isZero();
            });

            assertPaymentConsumerHasNotCommitted(inputOffset);
        } finally {
            removeOutboxInsertFailure();
        }

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            Payment payment = paymentRepository.findByOrderId(orderId).orElseThrow();
            assertThat(payment.getStatus()).isEqualTo(PaymentStatus.COMPLETED);
            assertThat(outboxEventRepository.findAllByAggregateId(payment.getId()))
                    .singleElement()
                    .satisfies(event -> assertThat(event.getPublishedAt()).isNotNull());
        });
        awaitPaymentConsumerCommitted(inputOffset);

        List<ConsumerRecord<String, SpecificRecord>> records = consumePaymentEvents(Set.of(orderId));
        assertThat(records).singleElement().satisfies(record -> {
            assertThat(record.value()).isInstanceOf(PaymentCompleted.class);
            assertThat(record.key()).isEqualTo(orderId.toString());
        });
    }

    @Test
    void duplicateEventIdDeliveryProducesExactlyOneSideEffect() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        OrderCreated order = orderCreated(eventId, orderId, "149.99");

        long lastInputOffset = sendOrderCreated(order);
        lastInputOffset = Math.max(lastInputOffset, sendOrderCreated(order));
        lastInputOffset = Math.max(lastInputOffset, sendOrderCreated(order));

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(paymentRepository.findByOrderId(orderId))
                    .hasValueSatisfying(payment -> {
                        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.COMPLETED);
                        assertThat(outboxEventRepository.findAllByAggregateId(payment.getId())).hasSize(1);
                    });
            assertThat(processedEventRepository.existsById(eventId)).isTrue();
        });
        awaitPaymentConsumerCommitted(lastInputOffset);

        // A distinct event id must create a distinct, independently tracked payment.
        UUID secondEventId = UUID.randomUUID();
        UUID secondOrderId = UUID.randomUUID();
        sendOrderCreated(orderCreated(secondEventId, secondOrderId, "149.99"));
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(paymentRepository.findByOrderId(secondOrderId))
                    .hasValueSatisfying(payment -> {
                        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.COMPLETED);
                        assertThat(outboxEventRepository.findAllByAggregateId(payment.getId())).hasSize(1);
                    });
            assertThat(processedEventRepository.existsById(secondEventId)).isTrue();
        });
        awaitPaymentConsumerCommitted(lastInputOffset + 1);

        List<ConsumerRecord<String, SpecificRecord>> records = consumePaymentEvents(Set.of(orderId, secondOrderId));
        assertThat(records).hasSize(2);
    }

    private List<ConsumerRecord<String, SpecificRecord>> consumePaymentEvents(Set<UUID> orderIds) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "payment-outbox-test-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroDeserializer.class.getName());
        properties.put("schema.registry.url", schemaRegistryUrl());
        properties.put("specific.avro.reader", true);

        Map<UUID, ConsumerRecord<String, SpecificRecord>> received = new HashMap<>();
        try (KafkaConsumer<String, SpecificRecord> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of(PAYMENTS_TOPIC));
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (System.nanoTime() < deadline && !received.keySet().containsAll(orderIds)) {
                consumer.poll(Duration.ofMillis(250)).forEach(record -> {
                    UUID orderId = paymentOrderId(record.value());
                    if (orderIds.contains(orderId)) {
                        received.putIfAbsent(orderId, record);
                    }
                });
            }
        }
        return new ArrayList<>(received.values());
    }

    private UUID paymentOrderId(SpecificRecord event) {
        if (event instanceof PaymentCompleted completed) {
            return completed.getPayload().getOrderId();
        }
        if (event instanceof PaymentFailed failed) {
            return failed.getPayload().getOrderId();
        }
        return null;
    }

    private long sendOrderCreated(OrderCreated orderCreated) throws Exception {
        return kafkaTemplate.send(new ProducerRecord<>(
                "orders", orderCreated.getAggregateId().toString(), orderCreated))
                .get(10, TimeUnit.SECONDS)
                .getRecordMetadata()
                .offset();
    }

    private void awaitPaymentConsumerCommitted(long recordOffset) {
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            try (AdminClient adminClient = adminClient()) {
                var offsets = adminClient.listConsumerGroupOffsets(PAYMENT_CONSUMER_GROUP)
                        .partitionsToOffsetAndMetadata()
                        .get();
                var committedOffset = offsets.get(new TopicPartition("orders", 0));
                assertThat(committedOffset).isNotNull();
                assertThat(committedOffset.offset()).isGreaterThan(recordOffset);
            }
        });
    }

    private void assertPaymentConsumerHasNotCommitted(long recordOffset) throws Exception {
        try (AdminClient adminClient = adminClient()) {
            var offsets = adminClient.listConsumerGroupOffsets(PAYMENT_CONSUMER_GROUP)
                    .partitionsToOffsetAndMetadata()
                    .get();
            var committedOffset = offsets.get(new TopicPartition("orders", 0));
            assertThat(committedOffset == null || committedOffset.offset() <= recordOffset).isTrue();
        }
    }

    private AdminClient adminClient() {
        return AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafka.getBootstrapServers()));
    }

    private void installOutboxInsertFailure() {
        jdbcTemplate.execute("CREATE SEQUENCE payment_service.payment_outbox_reject_attempts");
        jdbcTemplate.execute("""
                CREATE FUNCTION payment_service.reject_payment_outbox_insert() RETURNS trigger AS $$
                BEGIN
                    PERFORM nextval('payment_service.payment_outbox_reject_attempts');
                    RAISE EXCEPTION 'forced payment outbox persistence failure';
                END;
                $$ LANGUAGE plpgsql
                """);
        jdbcTemplate.execute("""
                CREATE TRIGGER reject_payment_outbox_insert
                BEFORE INSERT ON payment_service.outbox_events
                FOR EACH ROW EXECUTE FUNCTION payment_service.reject_payment_outbox_insert()
                """);
    }

    private void removeOutboxInsertFailure() {
        jdbcTemplate.execute(
                "DROP TRIGGER IF EXISTS reject_payment_outbox_insert ON payment_service.outbox_events");
        jdbcTemplate.execute("DROP FUNCTION IF EXISTS payment_service.reject_payment_outbox_insert()");
        jdbcTemplate.execute("DROP SEQUENCE IF EXISTS payment_service.payment_outbox_reject_attempts");
    }

    private OrderCreated orderCreated(UUID orderId, String amount) {
        return orderCreated(UUID.randomUUID(), orderId, amount);
    }

    private OrderCreated orderCreated(UUID eventId, UUID orderId, String amount) {
        return OrderCreated.newBuilder()
                .setEventId(eventId)
                .setEventType("OrderCreated")
                .setEventVersion(1)
                .setOccurredAt(Instant.now().truncatedTo(ChronoUnit.MILLIS))
                .setAggregateType("Order")
                .setAggregateId(orderId)
                .setPayload(OrderCreatedPayload.newBuilder()
                        .setOrderId(orderId)
                        .setCustomerId(UUID.randomUUID())
                        .setTotalAmount(new BigDecimal(amount))
                        .setCurrency("USD")
                        .build())
                .build();
    }

    private String headerValue(ConsumerRecord<String, SpecificRecord> record, String name) {
        return new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }

    private static String schemaRegistryUrl() {
        return "http://%s:%d".formatted(schemaRegistry.getHost(), schemaRegistry.getMappedPort(8081));
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class KafkaTopicConfiguration {

        @Bean
        NewTopics eventTopics() {
            return new NewTopics(
                    TopicBuilder.name("orders").partitions(1).replicas(1).build(),
                    TopicBuilder.name(PAYMENTS_TOPIC).partitions(1).replicas(1).build());
        }
    }
}
