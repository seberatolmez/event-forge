package com.eventforge.payment.domain;

import java.math.BigDecimal;
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
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "payments", schema = "payment_service")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Payment {

    private static final int MAX_REASON_LENGTH = 255;

    @Id
    @UuidGenerator
    @Column(nullable = false, updatable = false)
    private UUID id;

    @Column(name = "order_id", nullable = false, unique = true, updatable = false)
    private UUID orderId;

    @Column(nullable = false, precision = 12, scale = 2, updatable = false)
    private BigDecimal amount;

    @Column(nullable = false, length = 3, updatable = false)
    private String currency;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private PaymentStatus status;

    @Column(name = "provider_reference", length = 255)
    private String providerReference;

    @Column(name = "failure_reason", length = MAX_REASON_LENGTH)
    private String failureReason;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    private Payment(UUID orderId, BigDecimal amount, String currency) {
        this.orderId = orderId;
        this.amount = amount.setScale(2);
        this.currency = currency;
        this.status = PaymentStatus.PENDING;
    }

    public static Payment create(UUID orderId, BigDecimal amount, String currency) {
        if (orderId == null) {
            throw new InvalidPaymentException("orderId must not be null");
        }
        if (amount == null || amount.signum() <= 0) {
            throw new InvalidPaymentException("amount must be greater than zero");
        }
        if (amount.scale() > 2) {
            throw new InvalidPaymentException("amount must have no more than 2 decimal places");
        }
        BigDecimal normalizedAmount = amount.setScale(2);
        if (normalizedAmount.precision() > 12) {
            throw new InvalidPaymentException("amount must fit NUMERIC(12, 2)");
        }
        if (currency == null || !currency.matches("[A-Z]{3}")) {
            throw new InvalidPaymentException("currency must be a 3-letter ISO 4217 code");
        }
        return new Payment(orderId, normalizedAmount, currency);
    }

    public void complete(String providerReference) {
        if (providerReference == null || providerReference.isBlank()) {
            throw new InvalidPaymentException("providerReference must not be blank");
        }
        if (providerReference.length() > 255) {
            throw new InvalidPaymentException("providerReference must be at most 255 characters");
        }
        ensurePending();
        this.providerReference = providerReference;
        this.status = PaymentStatus.COMPLETED;
    }

    public void fail(String reason) {
        if (reason == null || reason.isBlank()) {
            throw new InvalidPaymentException("failure reason must not be blank");
        }
        if (reason.length() > MAX_REASON_LENGTH) {
            throw new InvalidPaymentException("failure reason must be at most 255 characters");
        }
        ensurePending();
        this.failureReason = reason;
        this.status = PaymentStatus.FAILED;
    }

    private void ensurePending() {
        if (status != PaymentStatus.PENDING) {
            throw new InvalidPaymentTransitionException(
                    "payment in status %s cannot transition to a final outcome".formatted(status));
        }
    }
}
