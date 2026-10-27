package com.eventforge.order.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;
import org.hibernate.annotations.UuidGenerator;

import jakarta.persistence.Column;
import jakarta.persistence.CollectionTable;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.Table;

@Entity
@Table(name = "orders", schema = "order_service")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
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

    @ElementCollection
    @CollectionTable(name = "order_items", schema = "order_service", joinColumns = @JoinColumn(name = "order_id"))
    private List<OrderItem> items = new ArrayList<>();

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    private Order(UUID customerId, BigDecimal totalAmount, String currency, List<OrderItem> items) {
        this.id = null;
        this.customerId = customerId;
        this.totalAmount = totalAmount;
        this.currency = currency;
        this.status = OrderStatus.PENDING;
        this.items = new ArrayList<>(items);
    }

    public static Order create(UUID customerId, BigDecimal totalAmount, String currency, List<OrderItem> items) {
        if (customerId == null) {
            throw new InvalidOrderException("customerId must not be null");
        }
        if (totalAmount == null || totalAmount.signum() <= 0) {
            throw new InvalidOrderException("totalAmount must be greater than zero");
        }
        if (currency == null || !currency.matches("[A-Z]{3}")) {
            throw new InvalidOrderException("currency must be a 3-letter ISO 4217 code");
        }
        if (items == null || items.isEmpty() || items.stream().anyMatch(item -> item == null)) {
            throw new InvalidOrderException("items must contain at least one valid item");
        }
        if (items.stream().map(OrderItem::getProductId).distinct().count() != items.size()) {
            throw new InvalidOrderException("items must not contain duplicate productId values");
        }
        return new Order(customerId, totalAmount, currency, items);
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
}
