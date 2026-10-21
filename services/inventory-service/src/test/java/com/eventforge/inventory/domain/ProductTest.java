package com.eventforge.inventory.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class ProductTest {

    @Test
    void reserveMovesQuantityFromAvailableToReserved() {
        Product product = Product.create("SKU-1", "Test product", 5);

        product.reserve(2);

        assertThat(product.getAvailableQuantity()).isEqualTo(3);
        assertThat(product.getReservedQuantity()).isEqualTo(2);
    }

    @Test
    void reserveRejectsInsufficientStockWithoutChangingQuantities() {
        Product product = Product.create("SKU-1", "Test product", 1);

        assertThatThrownBy(() -> product.reserve(2)).isInstanceOf(IllegalArgumentException.class);

        assertThat(product.getAvailableQuantity()).isEqualTo(1);
        assertThat(product.getReservedQuantity()).isZero();
    }

    @Test
    void createRejectsInvalidInitialStock() {
        assertThatThrownBy(() -> Product.create("SKU-1", "Test product", -1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
