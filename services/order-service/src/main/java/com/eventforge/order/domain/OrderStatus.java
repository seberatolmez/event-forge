package com.eventforge.order.domain;

public enum OrderStatus {
    PENDING,
    PAYMENT_COMPLETED,
    PAYMENT_FAILED,
    INVENTORY_RESERVED,
    INVENTORY_FAILED,
    COMPLETED,
    CANCELLED
}
