package com.eventforge.inventory.domain;

import java.time.Instant;
import java.util.UUID;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;
import org.hibernate.annotations.UuidGenerator;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "products", schema = "inventory_service")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Product {

    @Id
    @UuidGenerator
    @Column(nullable = false, updatable = false)
    private UUID id;

    @Column(nullable = false, unique = true, length = 100, updatable = false)
    private String sku;

    @Column(nullable = false, length = 255, updatable = false)
    private String name;

    @Column(name = "available_quantity", nullable = false)
    private int availableQuantity;

    @Column(name = "reserved_quantity", nullable = false)
    private int reservedQuantity;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    private Product(String sku, String name, int availableQuantity) {
        this.sku = sku;
        this.name = name;
        this.availableQuantity = availableQuantity;
        this.reservedQuantity = 0;
    }

    public static Product create(String sku, String name, int availableQuantity) {
        if (sku == null || sku.isBlank() || sku.length() > 100) {
            throw new IllegalArgumentException("sku must contain 1 to 100 characters");
        }
        if (name == null || name.isBlank() || name.length() > 255) {
            throw new IllegalArgumentException("name must contain 1 to 255 characters");
        }
        if (availableQuantity < 0) {
            throw new IllegalArgumentException("availableQuantity must not be negative");
        }
        return new Product(sku, name, availableQuantity);
    }

    public void reserve(int quantity) {
        if (quantity < 1 || quantity > availableQuantity) {
            throw new IllegalArgumentException("quantity must be positive and available in stock");
        }
        int newReservedQuantity = Math.addExact(reservedQuantity, quantity);
        availableQuantity -= quantity;
        reservedQuantity = newReservedQuantity;
    }
}
