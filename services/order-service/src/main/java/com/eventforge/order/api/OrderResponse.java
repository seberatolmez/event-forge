package com.eventforge.order.api;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import com.eventforge.order.domain.Order;
import com.eventforge.order.domain.OrderItem;
import com.eventforge.order.domain.OrderStatus;

public record OrderResponse(
        UUID id,
        UUID customerId,
        BigDecimal totalAmount,
        String currency,
        OrderStatus status,
        Instant createdAt,
        Instant updatedAt,
        List<Item> items) {

    public static OrderResponse from(Order order) {
        return new OrderResponse(
                order.getId(),
                order.getCustomerId(),
                order.getTotalAmount(),
                order.getCurrency(),
                order.getStatus(),
                order.getCreatedAt(),
                order.getUpdatedAt(),
                order.getItems().stream().map(Item::from).toList());
    }

    public record Item(UUID productId, int quantity) {

        private static Item from(OrderItem item) {
            return new Item(item.getProductId(), item.getQuantity());
        }
    }
}
