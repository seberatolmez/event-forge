package com.eventforge.order.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin.NewTopics;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.eventforge.contracts.order.OrderCreated;
import com.eventforge.order.outbox.OutboxEvent;
import com.eventforge.order.outbox.OutboxEventRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "eventforge.outbox.publisher.enabled=true",
                "eventforge.outbox.publisher.poll-interval-ms=100",
                "eventforge.outbox.publisher.send-timeout-ms=5000",
                "spring.kafka.producer.properties[schema.registry.url]=mock://outbox-publisher-integration"
        })
@Import(OutboxPublisherIntegrationTest.KafkaTopicConfiguration.class)
@Testcontainers(disabledWithoutDocker = true)
class OutboxPublisherIntegrationTest {

    private static final String ORDERS_TOPIC = "orders";

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"));

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired
    TestRestTemplate restTemplate;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    OutboxEventRepository outboxEventRepository;

    @Test
    void orderCreatedOutboxEventIsPublishedToKafkaAndMarkedPublished() throws Exception {
        UUID customerId = UUID.randomUUID();
        ResponseEntity<String> response = postOrder(customerId);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        JsonNode createdOrder = objectMapper.readTree(response.getBody());
        UUID orderId = UUID.fromString(createdOrder.path("id").asText());

        ConsumerRecord<String, OrderCreated> record = consumeOrderCreatedEvent();

        assertThat(record.key()).isEqualTo(orderId.toString());
        assertThat(record.value().getEventType().toString()).isEqualTo("OrderCreated");
        assertThat(record.value().getEventVersion()).isEqualTo(1);
        assertThat(record.value().getAggregateType().toString()).isEqualTo("Order");
        assertThat(record.value().getAggregateId()).isEqualTo(orderId);
        assertThat(record.value().getPayload().getOrderId()).isEqualTo(orderId);
        assertThat(record.value().getPayload().getCustomerId()).isEqualTo(customerId);
        assertThat(record.value().getPayload().getTotalAmount()).isEqualByComparingTo("149.99");
        assertThat(record.value().getPayload().getCurrency().toString()).isEqualTo("USD");
        assertThat(record.value().getOccurredAt().toEpochMilli()).isEqualTo(
                Instant.parse(headerValue(record, "occurredAt")).toEpochMilli());
        assertThat(record.headers().lastHeader("eventId")).isNotNull();
        assertThat(headerValue(record, "eventType")).isEqualTo("OrderCreated");
        assertThat(headerValue(record, "eventVersion")).isEqualTo("1");
        assertThat(headerValue(record, "aggregateType")).isEqualTo("Order");
        assertThat(headerValue(record, "aggregateId")).isEqualTo(orderId.toString());

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            List<OutboxEvent> events = outboxEventRepository.findAllByAggregateId(orderId);
            assertThat(events).hasSize(1);
            assertThat(events.get(0).getPublishedAt()).isNotNull();
            assertThat(headerValue(record, "eventId")).isEqualTo(events.get(0).getId().toString());
            assertThat(headerValue(record, "occurredAt")).isEqualTo(events.get(0).getCreatedAt().toString());
        });
    }

    private String headerValue(ConsumerRecord<String, OrderCreated> record, String name) {
        return new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }

    private ConsumerRecord<String, OrderCreated> consumeOrderCreatedEvent() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "outbox-publisher-test-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroDeserializer.class.getName());
        properties.put("schema.registry.url", "mock://outbox-publisher-integration");
        properties.put("specific.avro.reader", true);

        try (KafkaConsumer<String, OrderCreated> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of(ORDERS_TOPIC));
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (System.nanoTime() < deadline) {
                var records = consumer.poll(Duration.ofMillis(250));
                if (!records.isEmpty()) {
                    return records.iterator().next();
                }
            }
        }
        throw new AssertionError("No OrderCreated event was received from Kafka within 10 seconds");
    }

    private ResponseEntity<String> postOrder(UUID customerId) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        String body = """
                {
                  "customerId": "%s",
                  "totalAmount": 149.99,
                  "currency": "USD"
                }
                """.formatted(customerId);
        return restTemplate.exchange(
                "/orders", HttpMethod.POST, new HttpEntity<>(body, headers), String.class);
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class KafkaTopicConfiguration {

        @Bean
        NewTopics ordersTopic() {
            return new NewTopics(TopicBuilder.name(ORDERS_TOPIC).partitions(1).replicas(1).build());
        }
    }
}
