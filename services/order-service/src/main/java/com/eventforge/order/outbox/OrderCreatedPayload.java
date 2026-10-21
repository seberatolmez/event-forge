package com.eventforge.order.outbox;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

public record OrderCreatedPayload(
        UUID orderId, UUID customerId, BigDecimal totalAmount, String currency, List<Item> items) {

    public record Item(UUID productId, int quantity) {
    }
}
