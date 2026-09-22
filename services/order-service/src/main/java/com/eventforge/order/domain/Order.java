package com.eventforge.order.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;
import org.hibernate.annotations.UuidGenerator;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "orders", schema = "order_service")
public class Order {

    private static final Map<OrderStatus, Set<OrderStatus>> ALLOWED_TRANSITIONS = Map.of(
            OrderStatus.PENDING, Set.of(
                    OrderStatus.PAYMENT_COMPLETED,
                    OrderStatus.PAYMENT_FAILED,
                    OrderStatus.CANCELLED),
            OrderStatus.PAYMENT_COMPLETED, Set.of(
                    OrderStatus.INVENTORY_RESERVED,
                    OrderStatus.INVENTORY_FAILED),
            OrderStatus.PAYMENT_FAILED, Set.of(
                    OrderStatus.CANCELLED),
            OrderStatus.INVENTORY_RESERVED, Set.of(
                    OrderStatus.COMPLETED),
            OrderStatus.INVENTORY_FAILED, Set.of(
                    OrderStatus.CANCELLED),
            OrderStatus.COMPLETED, Set.of(),
            OrderStatus.CANCELLED, Set.of());

    @Id
    @UuidGenerator
    @Column(nullable = false, updatable = false)
    private UUID id;

    @Column(name = "customer_id", nullable = false)
    private UUID customerId;

    @Column(name = "total_amount", nullable = false, precision = 12, scale = 2)
    private BigDecimal totalAmount;

    @Column(nullable = false, length = 3)
    private String currency;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 32)
    private OrderStatus status;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected Order() {
    }

    private Order(UUID customerId, BigDecimal totalAmount, String currency) {
        this.id = null;
        this.customerId = customerId;
        this.totalAmount = totalAmount;
        this.currency = currency;
        this.status = OrderStatus.PENDING;
    }

    public static Order create(UUID customerId, BigDecimal totalAmount, String currency) {
        if (customerId == null) {
            throw new InvalidOrderException("customerId must not be null");
        }
        if (totalAmount == null || totalAmount.signum() <= 0) {
            throw new InvalidOrderException("totalAmount must be greater than zero");
        }
        if (currency == null || !currency.matches("[A-Z]{3}")) {
            throw new InvalidOrderException("currency must be a 3-letter ISO 4217 code");
        }
        return new Order(customerId, totalAmount, currency);
    }

    public void transitionTo(OrderStatus target) {
        if (target == null) {
            throw new InvalidOrderTransitionException("target status must not be null");
        }
        if (status == target) {
            return;
        }
        if (!ALLOWED_TRANSITIONS.get(status).contains(target)) {
            throw new InvalidOrderTransitionException(
                    "illegal status transition from %s to %s".formatted(status, target));
        }
        this.status = target;
    }

    public boolean canTransitionTo(OrderStatus target) {
        return status == target || ALLOWED_TRANSITIONS.get(status).contains(target);
    }

    public UUID getId() {
        return id;
    }

    public UUID getCustomerId() {
        return customerId;
    }

    public BigDecimal getTotalAmount() {
        return totalAmount;
    }

    public String getCurrency() {
        return currency;
    }

    public OrderStatus getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
