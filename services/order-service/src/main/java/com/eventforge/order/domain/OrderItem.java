package com.eventforge.order.domain;

import java.util.UUID;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;

@Embeddable
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class OrderItem {

    @Column(name = "product_id", nullable = false)
    private UUID productId;

    @Column(nullable = false)
    private int quantity;

    private OrderItem(UUID productId, int quantity) {
        this.productId = productId;
        this.quantity = quantity;
    }

    public static OrderItem create(UUID productId, int quantity) {
        if (productId == null) {
            throw new InvalidOrderException("productId must not be null");
        }
        if (quantity < 1) {
            throw new InvalidOrderException("quantity must be greater than zero");
        }
        return new OrderItem(productId, quantity);
    }
}
