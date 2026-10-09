package com.eventforge.order.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.eventforge.order.outbox.OutboxEvent;
import com.eventforge.order.outbox.OutboxEventRepository;
import com.eventforge.order.repository.OrderRepository;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "eventforge.outbox.publisher.enabled=false")
@Testcontainers(disabledWithoutDocker = true)
class OrderOutboxRollbackIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired
    TestRestTemplate restTemplate;

    @Autowired
    OrderRepository orderRepository;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @MockitoBean
    OutboxEventRepository outboxEventRepository;

    @Test
    void orderIsRolledBackWhenOutboxWriteFails() {
        UUID customerId = UUID.randomUUID();
        doAnswer(invocation -> {
            orderRepository.flush();
            throw new IllegalStateException("simulated outbox persistence failure");
        }).when(outboxEventRepository).save(any(OutboxEvent.class));

        ResponseEntity<String> response = postOrder(customerId);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        Long persistedOrderCount = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM order_service.orders WHERE customer_id = ?",
                Long.class,
                customerId);
        assertThat(persistedOrderCount).isZero();
        verify(outboxEventRepository).save(any(OutboxEvent.class));
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
        return restTemplate.exchange("/orders", HttpMethod.POST, new HttpEntity<>(body, headers), String.class);
    }
}
