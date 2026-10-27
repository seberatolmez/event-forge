package com.eventforge.inventory.domain;

import java.util.UUID;

public final class InventoryEventPayload {

    private InventoryEventPayload() {
    }

    public record Reserved(UUID reservationId, UUID orderId, UUID productId, int quantity) {
    }

    public record Failed(UUID orderId, UUID productId, int quantity, String reason) {
    }
}
