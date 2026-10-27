package com.eventforge.notification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.eventforge.notification.domain.Notification;
import com.eventforge.notification.domain.NotificationStatus;
import com.eventforge.notification.repository.NotificationRepository;

@AutoConfigureMockMvc
@SpringBootTest(properties = "eventforge.notification.consumer.enabled=false")
@Testcontainers(disabledWithoutDocker = true)
class NotificationServiceApplicationTests {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private NotificationRepository notificationRepository;

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
    void notificationPersistsAsPendingAfterFlywayMigration() {
        Notification saved = notificationRepository.saveAndFlush(
                Notification.create(UUID.randomUUID(), "OrderCreated", UUID.randomUUID().toString()));

        Notification fetched = notificationRepository.findById(saved.getId()).orElseThrow();

        assertThat(fetched.getEventId()).isEqualTo(saved.getEventId());
        assertThat(fetched.getStatus()).isEqualTo(NotificationStatus.PENDING);
        assertThat(fetched.getCreatedAt()).isNotNull();
    }
}
