package com.eventforge.inventory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.eventforge.inventory.domain.Product;
import com.eventforge.inventory.repository.ProductRepository;

@AutoConfigureMockMvc
@SpringBootTest(properties = {
        "eventforge.inventory.consumer.enabled=false",
        "eventforge.outbox.publisher.enabled=false"
})
@Testcontainers(disabledWithoutDocker = true)
class InventoryServiceApplicationTests {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ProductRepository productRepository;

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
    void productPersistsWithInitialStockAfterFlywayMigration() {
        Product saved = productRepository.saveAndFlush(Product.create("SKU-1", "Test product", 5));

        Product fetched = productRepository.findById(saved.getId()).orElseThrow();

        assertThat(fetched.getSku()).isEqualTo("SKU-1");
        assertThat(fetched.getName()).isEqualTo("Test product");
        assertThat(fetched.getAvailableQuantity()).isEqualTo(5);
        assertThat(fetched.getReservedQuantity()).isZero();
        assertThat(fetched.getCreatedAt()).isNotNull();
        assertThat(fetched.getUpdatedAt()).isNotNull();
    }
}
