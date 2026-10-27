package com.eventforge.notification.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.apache.avro.specific.SpecificRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
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
import com.eventforge.contracts.payment.PaymentCompletedPayload;
import com.eventforge.notification.domain.Notification;
import com.eventforge.notification.domain.NotificationStatus;
import com.eventforge.notification.repository.NotificationRepository;

@SpringBootTest(properties = {
        "eventforge.notification.consumer.enabled=true",
        "eventforge.notification.consumer.group-id=notification-service-integration",
        "spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer",
        "spring.kafka.producer.value-serializer=io.confluent.kafka.serializers.KafkaAvroSerializer",
        "spring.kafka.producer.properties[value.subject.name.strategy]=io.confluent.kafka.serializers.subject.TopicRecordNameStrategy"
})
@Import(NotificationKafkaIntegrationTest.KafkaTopicConfiguration.class)
@Testcontainers(disabledWithoutDocker = true)
class NotificationKafkaIntegrationTest {

    private static final Network KAFKA_NETWORK = Network.newNetwork();

    @Container
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
        registry.add("spring.kafka.producer.properties[schema.registry.url]",
                NotificationKafkaIntegrationTest::schemaRegistryUrl);
        registry.add("spring.kafka.consumer.properties[schema.registry.url]",
                NotificationKafkaIntegrationTest::schemaRegistryUrl);
    }

    private static String schemaRegistryUrl() {
        return "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081);
    }

    @Autowired
    NotificationRepository notificationRepository;

    @Autowired
    KafkaTemplate<String, SpecificRecord> kafkaTemplate;

    @Test
    void businessEventsCreateSentNotificationsIdempotently() {
        UUID orderId = UUID.randomUUID();
        UUID customerId = UUID.randomUUID();
        OrderCreated orderCreated = orderCreated(orderId, customerId);

        send("orders", orderId.toString(), orderCreated);
        send("orders", orderId.toString(), orderCreated);
        send("payments", orderId.toString(), paymentCompleted(orderId));

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            List<Notification> notifications = notificationRepository.findAll();
            assertThat(notifications).hasSize(2);
            Notification orderNotification = notifications.stream()
                    .filter(n -> n.getType().equals("OrderCreated"))
                    .findFirst().orElseThrow();
            assertThat(orderNotification.getEventId()).isEqualTo(orderCreated.getEventId());
            assertThat(orderNotification.getRecipient()).isEqualTo(customerId.toString());
            assertThat(orderNotification.getStatus()).isEqualTo(NotificationStatus.SENT);
            assertThat(notifications).anySatisfy(n -> {
                assertThat(n.getType()).isEqualTo("PaymentCompleted");
                assertThat(n.getRecipient()).isEqualTo(orderId.toString());
                assertThat(n.getStatus()).isEqualTo(NotificationStatus.SENT);
            });
        });
    }

    private void send(String topic, String key, SpecificRecord event) {
        kafkaTemplate.send(topic, key, event);
    }

    private static OrderCreated orderCreated(UUID orderId, UUID customerId) {
        return OrderCreated.newBuilder()
                .setEventId(UUID.randomUUID())
                .setEventType("OrderCreated")
                .setEventVersion(2)
                .setOccurredAt(Instant.parse("2026-10-27T10:00:00.123Z"))
                .setAggregateType("Order")
                .setAggregateId(orderId)
                .setPayload(OrderCreatedPayload.newBuilder()
                        .setOrderId(orderId)
                        .setCustomerId(customerId)
                        .setTotalAmount(new BigDecimal("99.00"))
                        .setCurrency("USD")
                        .build())
                .build();
    }

    private static PaymentCompleted paymentCompleted(UUID orderId) {
        UUID paymentId = UUID.randomUUID();
        return PaymentCompleted.newBuilder()
                .setEventId(UUID.randomUUID())
                .setEventType("PaymentCompleted")
                .setEventVersion(1)
                .setOccurredAt(Instant.parse("2026-10-27T10:00:01.123Z"))
                .setAggregateType("Payment")
                .setAggregateId(paymentId)
                .setPayload(PaymentCompletedPayload.newBuilder()
                        .setPaymentId(paymentId)
                        .setOrderId(orderId)
                        .setAmount(new BigDecimal("99.00"))
                        .setCurrency("USD")
                        .setProviderReference("simulated-payment-123")
                        .build())
                .build();
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class KafkaTopicConfiguration {

        @Bean
        NewTopics notificationTopics() {
            return new NewTopics(
                    TopicBuilder.name("orders").build(),
                    TopicBuilder.name("payments").build(),
                    TopicBuilder.name("inventory").build());
        }
    }
}
