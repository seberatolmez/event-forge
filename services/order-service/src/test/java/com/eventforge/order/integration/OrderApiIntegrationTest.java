package com.eventforge.order.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.eventforge.order.outbox.OutboxEvent;
import com.eventforge.order.outbox.OutboxEventRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "eventforge.outbox.publisher.enabled=false")
@Testcontainers(disabledWithoutDocker = true)
class OrderApiIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate restTemplate;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    OutboxEventRepository outboxEventRepository;

    @Test
    void outboxEventPersistsMetadataAndJsonPayload() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID customerId = UUID.randomUUID();
        JsonNode payload = objectMapper.readTree("""
                {
                  "orderId": "%s",
                  "customerId": "%s",
                  "totalAmount": 149.99,
                  "currency": "USD"
                }
                """.formatted(orderId, customerId));

        OutboxEvent savedEvent = outboxEventRepository.saveAndFlush(
                OutboxEvent.create("Order", orderId, "OrderCreated", 1, payload));

        assertThat(savedEvent.getId()).isNotNull();
        assertThat(savedEvent.getAggregateType()).isEqualTo("Order");
        assertThat(savedEvent.getAggregateId()).isEqualTo(orderId);
        assertThat(savedEvent.getEventType()).isEqualTo("OrderCreated");
        assertThat(savedEvent.getEventVersion()).isEqualTo(1);
        assertThat(savedEvent.getPayload()).isEqualTo(payload);
        assertThat(savedEvent.getCreatedAt()).isNotNull();
        assertThat(savedEvent.getPublishedAt()).isNull();
        assertThat(savedEvent.getRetryCount()).isZero();
    }

    @Test
    void createdOrderAndOutboxEventArePersistedAndFetchableAgainstRealPostgres() throws Exception {
        UUID customerId = UUID.randomUUID();
        ResponseEntity<String> createResponse = postOrder(customerId, "149.99", "USD");

        assertThat(createResponse.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        JsonNode created = objectMapper.readTree(createResponse.getBody());
        UUID orderId = UUID.fromString(created.path("id").asText());
        assertThat(created.path("customerId").asText()).isEqualTo(customerId.toString());
        assertThat(created.path("totalAmount").decimalValue()).isEqualByComparingTo(new BigDecimal("149.99"));
        assertThat(created.path("currency").asText()).isEqualTo("USD");
        assertThat(created.path("status").asText()).isEqualTo("PENDING");

        List<OutboxEvent> outboxEvents = outboxEventRepository.findAllByAggregateId(orderId);
        assertThat(outboxEvents).hasSize(1);
        OutboxEvent outboxEvent = outboxEvents.get(0);
        assertThat(outboxEvent.getAggregateType()).isEqualTo("Order");
        assertThat(outboxEvent.getEventType()).isEqualTo("OrderCreated");
        assertThat(outboxEvent.getEventVersion()).isEqualTo(1);
        assertThat(outboxEvent.getPayload().path("orderId").asText()).isEqualTo(orderId.toString());
        assertThat(outboxEvent.getPayload().path("customerId").asText()).isEqualTo(customerId.toString());
        assertThat(outboxEvent.getPayload().path("totalAmount").decimalValue())
                .isEqualByComparingTo(new BigDecimal("149.99"));
        assertThat(outboxEvent.getPayload().path("currency").asText()).isEqualTo("USD");

        assertThat(createResponse.getHeaders().getLocation().toString())
                .isEqualTo("http://localhost:%d/orders/%s".formatted(port, orderId));

        ResponseEntity<String> getResponse = restTemplate.getForEntity("/orders/{id}", String.class, orderId);

        assertThat(getResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        JsonNode fetched = objectMapper.readTree(getResponse.getBody());
        assertThat(fetched.path("id").asText()).isEqualTo(orderId.toString());
        assertThat(fetched.path("status").asText()).isEqualTo("PENDING");
        assertThat(fetched.path("customerId").asText()).isEqualTo(customerId.toString());
    }

    @Test
    void unknownOrderIdReturnsNotFound() {
        UUID unknownId = UUID.randomUUID();

        ResponseEntity<String> response = restTemplate.getForEntity("/orders/{id}", String.class, unknownId);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(response.getBody()).contains("not found");
    }

    @Test
    void zeroAmountIsRejected() {
        ResponseEntity<String> response = postOrder(UUID.randomUUID(), "0.00", "USD");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).contains("totalAmount");
    }

    @Test
    void invalidCurrencyIsRejected() {
        ResponseEntity<String> response = postOrder(UUID.randomUUID(), "10.00", "usd");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).contains("currency");
    }

    private ResponseEntity<String> postOrder(UUID customerId, String amount, String currency) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        String body = """
                {
                  "customerId": "%s",
                  "totalAmount": %s,
                  "currency": "%s"
                }
                """.formatted(customerId, amount, currency);
        return restTemplate.exchange("/orders", HttpMethod.POST, new HttpEntity<>(body, headers), String.class);
    }
}
