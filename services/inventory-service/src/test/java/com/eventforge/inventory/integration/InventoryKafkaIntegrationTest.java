package com.eventforge.inventory.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
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

import com.eventforge.contracts.inventory.InventoryReservationFailed;
import com.eventforge.contracts.inventory.InventoryReserved;
import com.eventforge.contracts.order.OrderCreated;
import com.eventforge.contracts.order.OrderCreatedPayload;
import com.eventforge.contracts.order.OrderItemPayload;
import com.eventforge.inventory.dedup.ProcessedEventRepository;
import com.eventforge.inventory.domain.Product;
import com.eventforge.inventory.outbox.OutboxEventRepository;
import com.eventforge.inventory.repository.InventoryReservationRepository;
import com.eventforge.inventory.repository.ProductRepository;
import com.eventforge.inventory.service.InventoryProcessor;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

@SpringBootTest(properties = {
        "eventforge.inventory.consumer.enabled=true",
        "eventforge.inventory.consumer.group-id=inventory-service-integration",
        "eventforge.inventory.consumer.retry-backoff-ms=100",
        "eventforge.outbox.publisher.enabled=true",
        "eventforge.outbox.publisher.poll-interval-ms=100",
        "eventforge.outbox.publisher.send-timeout-ms=5000"
})
@Import(InventoryKafkaIntegrationTest.KafkaTopicConfiguration.class)
@Testcontainers(disabledWithoutDocker = true)
class InventoryKafkaIntegrationTest {

    private static final String INVENTORY_TOPIC = "inventory";
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
        registry.add("spring.kafka.producer.properties[schema.registry.url]", InventoryKafkaIntegrationTest::schemaRegistryUrl);
        registry.add("spring.kafka.consumer.properties[schema.registry.url]", InventoryKafkaIntegrationTest::schemaRegistryUrl);
    }

    @Autowired
    ProductRepository productRepository;

    @Autowired
    InventoryReservationRepository reservationRepository;

    @Autowired
    OutboxEventRepository outboxEventRepository;

    @Autowired
    KafkaTemplate<String, SpecificRecord> kafkaTemplate;

    @Autowired
    InventoryProcessor inventoryProcessor;

    @Autowired
    ProcessedEventRepository processedEventRepository;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Test
    void consumesOrdersIdempotentlyAndPublishesReservedAndFailedEvents() throws Exception {
        Product product = productRepository.saveAndFlush(Product.create("SKU-" + UUID.randomUUID(), "Test product", 5));
        UUID reservedOrderId = UUID.randomUUID();
        UUID failedOrderId = UUID.randomUUID();
        OrderCreated reservedOrder = orderCreated(reservedOrderId, UUID.randomUUID(), product.getId(), 2);
        OrderCreated failedOrder = orderCreated(failedOrderId, UUID.randomUUID(), product.getId(), 4);

        sendOrderCreated(reservedOrder);
        sendOrderCreated(reservedOrder);
        sendOrderCreated(failedOrder);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            Product updated = productRepository.findById(product.getId()).orElseThrow();
            assertThat(updated.getAvailableQuantity()).isEqualTo(3);
            assertThat(updated.getReservedQuantity()).isEqualTo(2);
            assertThat(reservationRepository.findAllByOrderId(reservedOrderId)).hasSize(1);
            assertThat(outboxEventRepository.findAllByAggregateId(reservedOrderId))
                    .singleElement().satisfies(event -> assertThat(event.getPublishedAt()).isNotNull());
            assertThat(outboxEventRepository.findAllByAggregateId(failedOrderId))
                    .singleElement().satisfies(event -> assertThat(event.getPublishedAt()).isNotNull());
        });

        List<ConsumerRecord<String, SpecificRecord>> records = consumeInventoryEvents(Set.of(reservedOrderId, failedOrderId));

        assertThat(records).hasSize(2);
        assertThat(records).anySatisfy(record -> {
            assertThat(record.value()).isInstanceOf(InventoryReserved.class);
            InventoryReserved event = (InventoryReserved) record.value();
            assertThat(record.key()).isEqualTo(reservedOrderId.toString());
            assertThat(event.getPayload().getOrderId()).isEqualTo(reservedOrderId);
            assertThat(event.getPayload().getProductId()).isEqualTo(product.getId());
            assertThat(event.getPayload().getQuantity()).isEqualTo(2);
        });
        assertThat(records).anySatisfy(record -> {
            assertThat(record.value()).isInstanceOf(InventoryReservationFailed.class);
            InventoryReservationFailed event = (InventoryReservationFailed) record.value();
            assertThat(record.key()).isEqualTo(failedOrderId.toString());
            assertThat(event.getPayload().getOrderId()).isEqualTo(failedOrderId);
            assertThat(event.getPayload().getReason().toString()).contains("insufficient stock");
        });

        try (SchemaRegistryClient registryClient = new CachedSchemaRegistryClient(schemaRegistryUrl(), 10)) {
            assertThat(registryClient.getAllSubjects()).contains(
                    "inventory-com.eventforge.contracts.inventory.InventoryReserved",
                    "inventory-com.eventforge.contracts.inventory.InventoryReservationFailed");
        }
    }

    @Test
    void concurrentOrdersCannotReserveMoreThanAvailableStock() throws Exception {
        Product product = productRepository.saveAndFlush(Product.create("SKU-" + UUID.randomUUID(), "Limited stock", 5));
        OrderCreated firstOrder = orderCreated(UUID.randomUUID(), UUID.randomUUID(), product.getId(), 4);
        OrderCreated secondOrder = orderCreated(UUID.randomUUID(), UUID.randomUUID(), product.getId(), 4);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);

        try (var executor = Executors.newFixedThreadPool(2)) {
            var first = executor.submit(() -> processTogether(firstOrder, ready, start));
            var second = executor.submit(() -> processTogether(secondOrder, ready, start));
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            start.countDown();

            assertThat(first.get(10, TimeUnit.SECONDS)).isTrue();
            assertThat(second.get(10, TimeUnit.SECONDS)).isTrue();
        }

        Product updated = productRepository.findById(product.getId()).orElseThrow();
        assertThat(updated.getAvailableQuantity()).isEqualTo(1);
        assertThat(updated.getReservedQuantity()).isEqualTo(4);
        assertThat(reservationRepository.findAllByOrderId(firstOrder.getAggregateId()).size()
                + reservationRepository.findAllByOrderId(secondOrder.getAggregateId()).size()).isEqualTo(1);
    }

    @Test
    void outboxFailureRollsBackReservationAndAllowsTheEventToRetry() {
        Product product = productRepository.saveAndFlush(Product.create("SKU-" + UUID.randomUUID(), "Retry item", 5));
        OrderCreated order = orderCreated(UUID.randomUUID(), UUID.randomUUID(), product.getId(), 2);
        installOutboxFailureTrigger();
        try {
            assertThatThrownBy(() -> inventoryProcessor.processOrderCreated(order))
                    .isInstanceOf(RuntimeException.class);
        } finally {
            removeOutboxFailureTrigger();
        }

        Product unchanged = productRepository.findById(product.getId()).orElseThrow();
        assertThat(unchanged.getAvailableQuantity()).isEqualTo(5);
        assertThat(unchanged.getReservedQuantity()).isZero();
        assertThat(reservationRepository.findAllByOrderId(order.getAggregateId())).isEmpty();
        assertThat(processedEventRepository.existsById(order.getEventId())).isFalse();

        assertThat(inventoryProcessor.processOrderCreated(order)).isTrue();
        Product retried = productRepository.findById(product.getId()).orElseThrow();
        assertThat(retried.getAvailableQuantity()).isEqualTo(3);
        assertThat(reservationRepository.findAllByOrderId(order.getAggregateId())).hasSize(1);
    }

    private void installOutboxFailureTrigger() {
        jdbcTemplate.execute("""
                CREATE FUNCTION inventory_service.reject_inventory_outbox_insert() RETURNS trigger AS $$
                BEGIN
                    RAISE EXCEPTION 'forced inventory outbox failure';
                END;
                $$ LANGUAGE plpgsql
                """);
        jdbcTemplate.execute("""
                CREATE TRIGGER reject_inventory_outbox_insert
                BEFORE INSERT ON inventory_service.outbox_events
                FOR EACH ROW EXECUTE FUNCTION inventory_service.reject_inventory_outbox_insert()
                """);
    }

    private void removeOutboxFailureTrigger() {
        jdbcTemplate.execute(
                "DROP TRIGGER IF EXISTS reject_inventory_outbox_insert ON inventory_service.outbox_events");
        jdbcTemplate.execute("DROP FUNCTION IF EXISTS inventory_service.reject_inventory_outbox_insert()");
    }

    private boolean processTogether(OrderCreated event, CountDownLatch ready, CountDownLatch start) throws Exception {
        ready.countDown();
        if (!start.await(5, TimeUnit.SECONDS)) {
            throw new IllegalStateException("concurrent reservation test did not start");
        }
        return inventoryProcessor.processOrderCreated(event);
    }

    private void sendOrderCreated(OrderCreated orderCreated) throws Exception {
        kafkaTemplate.send(new ProducerRecord<>("orders", orderCreated.getAggregateId().toString(), orderCreated))
                .get(10, TimeUnit.SECONDS)
                .getRecordMetadata();
    }

    private List<ConsumerRecord<String, SpecificRecord>> consumeInventoryEvents(Set<UUID> orderIds) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "inventory-events-test-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroDeserializer.class.getName());
        properties.put("schema.registry.url", schemaRegistryUrl());
        properties.put("specific.avro.reader", true);

        Map<UUID, ConsumerRecord<String, SpecificRecord>> received = new HashMap<>();
        try (KafkaConsumer<String, SpecificRecord> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of(INVENTORY_TOPIC));
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (System.nanoTime() < deadline && !received.keySet().containsAll(orderIds)) {
                consumer.poll(Duration.ofMillis(250)).forEach(record -> {
                    UUID orderId = inventoryOrderId(record.value());
                    if (orderIds.contains(orderId)) {
                        received.putIfAbsent(orderId, record);
                    }
                });
            }
        }
        return new ArrayList<>(received.values());
    }

    private UUID inventoryOrderId(SpecificRecord event) {
        if (event instanceof InventoryReserved reserved) {
            return reserved.getPayload().getOrderId();
        }
        if (event instanceof InventoryReservationFailed failed) {
            return failed.getPayload().getOrderId();
        }
        return null;
    }

    private OrderCreated orderCreated(UUID orderId, UUID eventId, UUID productId, int quantity) {
        return OrderCreated.newBuilder()
                .setEventId(eventId)
                .setEventType("OrderCreated")
                .setEventVersion(2)
                .setOccurredAt(Instant.now().truncatedTo(ChronoUnit.MILLIS))
                .setAggregateType("Order")
                .setAggregateId(orderId)
                .setPayload(OrderCreatedPayload.newBuilder()
                        .setOrderId(orderId)
                        .setCustomerId(UUID.randomUUID())
                        .setTotalAmount(new java.math.BigDecimal("10.00"))
                        .setCurrency("USD")
                        .setItems(List.of(OrderItemPayload.newBuilder()
                                .setProductId(productId)
                                .setQuantity(quantity)
                                .build()))
                        .build())
                .build();
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
                    TopicBuilder.name(INVENTORY_TOPIC).partitions(1).replicas(1).build());
        }
    }
}
