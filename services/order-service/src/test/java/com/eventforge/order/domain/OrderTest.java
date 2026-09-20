package com.eventforge.order.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class OrderTest {

    private static final UUID CUSTOMER_ID = UUID.randomUUID();
    private static final BigDecimal AMOUNT = new BigDecimal("99.90");

    @Test
    void createInitializesPendingOrder() {
        Order order = Order.create(CUSTOMER_ID, AMOUNT, "USD");

        assertThat(order.getCustomerId()).isEqualTo(CUSTOMER_ID);
        assertThat(order.getTotalAmount()).isEqualByComparingTo(AMOUNT);
        assertThat(order.getCurrency()).isEqualTo("USD");
        assertThat(order.getStatus()).isEqualTo(OrderStatus.PENDING);
        assertThat(order.getId()).isNull();
    }

    @Test
    void createRejectsNullCustomerId() {
        assertThatThrownBy(() -> Order.create(null, AMOUNT, "USD"))
                .isInstanceOf(InvalidOrderException.class)
                .hasMessageContaining("customerId");
    }

    @Test
    void createRejectsNullAmount() {
        assertThatThrownBy(() -> Order.create(CUSTOMER_ID, null, "USD"))
                .isInstanceOf(InvalidOrderException.class)
                .hasMessageContaining("totalAmount");
    }

    @Test
    void createRejectsZeroAndNegativeAmount() {
        assertThatThrownBy(() -> Order.create(CUSTOMER_ID, BigDecimal.ZERO, "USD"))
                .isInstanceOf(InvalidOrderException.class);

        assertThatThrownBy(() -> Order.create(CUSTOMER_ID, new BigDecimal("-1.00"), "USD"))
                .isInstanceOf(InvalidOrderException.class);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = { "usd", "USDD", "US", "US D" })
    void createRejectsInvalidCurrency(String currency) {
        assertThatThrownBy(() -> Order.create(CUSTOMER_ID, AMOUNT, currency))
                .isInstanceOf(InvalidOrderException.class)
                .hasMessageContaining("currency");
    }

    @Test
    void successfulOrderLifecycleIsAllowed() {
        Order order = Order.create(CUSTOMER_ID, AMOUNT, "USD");

        order.transitionTo(OrderStatus.PAYMENT_COMPLETED);
        order.transitionTo(OrderStatus.INVENTORY_RESERVED);
        order.transitionTo(OrderStatus.COMPLETED);

        assertThat(order.getStatus()).isEqualTo(OrderStatus.COMPLETED);
    }

    @ParameterizedTest
    @CsvSource({
            "PENDING,PAYMENT_COMPLETED",
            "PENDING,PAYMENT_FAILED",
            "PENDING,CANCELLED",
            "PAYMENT_COMPLETED,INVENTORY_RESERVED",
            "PAYMENT_COMPLETED,INVENTORY_FAILED",
            "PAYMENT_FAILED,CANCELLED",
            "INVENTORY_RESERVED,COMPLETED",
            "INVENTORY_FAILED,CANCELLED"
    })
    void allowedTransitionsSucceed(OrderStatus from, OrderStatus to) {
        Order order = orderInStatus(from);
        order.transitionTo(to);
        assertThat(order.getStatus()).isEqualTo(to);
    }

    @ParameterizedTest
    @CsvSource({
            "PENDING,INVENTORY_RESERVED",
            "PENDING,COMPLETED",
            "PAYMENT_COMPLETED,PENDING",
            "PAYMENT_COMPLETED,COMPLETED",
            "PAYMENT_FAILED,PAYMENT_COMPLETED",
            "INVENTORY_RESERVED,PENDING",
            "COMPLETED,PENDING",
            "CANCELLED,PENDING"
    })
    void illegalTransitionsAreRejected(OrderStatus from, OrderStatus to) {
        Order order = orderInStatus(from);

        assertThatThrownBy(() -> order.transitionTo(to))
                .isInstanceOf(InvalidOrderTransitionException.class);
        assertThat(order.getStatus()).isEqualTo(from);
    }

    @ParameterizedTest
    @ValueSource(strings = { "COMPLETED", "CANCELLED" })
    void terminalStatesCannotTransitionToAnyOtherStatus(OrderStatus terminalStatus) {
        Order order = orderInStatus(terminalStatus);

        for (OrderStatus target : OrderStatus.values()) {
            if (target != terminalStatus) {
                assertThat(order.canTransitionTo(target)).isFalse();
                assertThatThrownBy(() -> order.transitionTo(target))
                        .isInstanceOf(InvalidOrderTransitionException.class);
            }
        }
    }

    @Test
    void transitionToSameStatusIsIdempotent() {
        Order order = Order.create(CUSTOMER_ID, AMOUNT, "USD");

        order.transitionTo(OrderStatus.PENDING);

        assertThat(order.getStatus()).isEqualTo(OrderStatus.PENDING);
    }

    @Test
    void transitionToNullIsRejected() {
        Order order = Order.create(CUSTOMER_ID, AMOUNT, "USD");

        assertThatThrownBy(() -> order.transitionTo(null))
                .isInstanceOf(InvalidOrderTransitionException.class);
    }

    private Order orderInStatus(OrderStatus status) {
        Order order = Order.create(CUSTOMER_ID, AMOUNT, "USD");
        switch (status) {
            case PENDING -> { }
            case PAYMENT_COMPLETED -> order.transitionTo(OrderStatus.PAYMENT_COMPLETED);
            case PAYMENT_FAILED -> order.transitionTo(OrderStatus.PAYMENT_FAILED);
            case INVENTORY_RESERVED -> {
                order.transitionTo(OrderStatus.PAYMENT_COMPLETED);
                order.transitionTo(OrderStatus.INVENTORY_RESERVED);
            }
            case INVENTORY_FAILED -> {
                order.transitionTo(OrderStatus.PAYMENT_COMPLETED);
                order.transitionTo(OrderStatus.INVENTORY_FAILED);
            }
            case COMPLETED -> {
                order.transitionTo(OrderStatus.PAYMENT_COMPLETED);
                order.transitionTo(OrderStatus.INVENTORY_RESERVED);
                order.transitionTo(OrderStatus.COMPLETED);
            }
            case CANCELLED -> order.transitionTo(OrderStatus.CANCELLED);
        }
        return order;
    }
}
