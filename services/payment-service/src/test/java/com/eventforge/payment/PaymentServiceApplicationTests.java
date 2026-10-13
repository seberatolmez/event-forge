package com.eventforge.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.math.BigDecimal;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.eventforge.payment.domain.Payment;
import com.eventforge.payment.domain.PaymentStatus;
import com.eventforge.payment.repository.PaymentRepository;

@AutoConfigureMockMvc
@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
class PaymentServiceApplicationTests {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private PaymentRepository paymentRepository;

    @Test
    void contextLoads() {
    }

    @Test
    void healthEndpointIsAvailable() throws Exception {
        mockMvc.perform(get("/actuator/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"));
    }

    @Test
    void paymentPersistsAgainstPostgresAfterFlywayMigration() {
        UUID orderId = UUID.randomUUID();
        Payment saved = paymentRepository.saveAndFlush(
                Payment.create(orderId, new BigDecimal("149.99"), "USD"));

        Payment fetched = paymentRepository.findByOrderId(orderId).orElseThrow();

        assertThat(fetched.getId()).isEqualTo(saved.getId());
        assertThat(fetched.getAmount()).isEqualByComparingTo("149.99");
        assertThat(fetched.getCurrency()).isEqualTo("USD");
        assertThat(fetched.getStatus()).isEqualTo(PaymentStatus.PENDING);
        assertThat(fetched.getCreatedAt()).isNotNull();
        assertThat(fetched.getUpdatedAt()).isNotNull();
    }

    @Test
    void uniqueOrderIdConstraintPreventsDuplicatePayments() {
        UUID orderId = UUID.randomUUID();
        paymentRepository.saveAndFlush(Payment.create(orderId, new BigDecimal("149.99"), "USD"));

        assertThatThrownBy(() -> paymentRepository.saveAndFlush(
                Payment.create(orderId, new BigDecimal("149.99"), "USD")))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

}
