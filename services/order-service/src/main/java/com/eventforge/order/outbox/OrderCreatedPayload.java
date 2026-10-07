package com.eventforge.order.outbox;

import java.math.BigDecimal;
import java.util.UUID;

public record OrderCreatedPayload(UUID orderId, UUID customerId, BigDecimal totalAmount, String currency) {
}
