package com.eventforge.payment.outbox;

import java.math.BigDecimal;
import java.util.UUID;

public final class PaymentEventPayload {

    private PaymentEventPayload() {
    }

    public record Completed(
            UUID paymentId,
            UUID orderId,
            BigDecimal amount,
            String currency,
            String providerReference) {
    }

    public record Failed(UUID paymentId, UUID orderId, String reason) {
    }
}
