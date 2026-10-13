package com.eventforge.payment.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class PaymentTest {

    private static final UUID ORDER_ID = UUID.randomUUID();
    private static final BigDecimal AMOUNT = new BigDecimal("149.99");

    @Test
    void createInitializesPendingPaymentAndNormalizesAmountScale() {
        Payment payment = Payment.create(ORDER_ID, new BigDecimal("149"), "USD");

        assertThat(payment.getOrderId()).isEqualTo(ORDER_ID);
        assertThat(payment.getAmount()).isEqualByComparingTo("149.00");
        assertThat(payment.getAmount().scale()).isEqualTo(2);
        assertThat(payment.getCurrency()).isEqualTo("USD");
        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.PENDING);
        assertThat(payment.getId()).isNull();
    }

    @Test
    void createRejectsNullOrderId() {
        assertThatThrownBy(() -> Payment.create(null, AMOUNT, "USD"))
                .isInstanceOf(InvalidPaymentException.class)
                .hasMessageContaining("orderId");
    }

    @Test
    void createRejectsNullZeroAndNegativeAmount() {
        assertThatThrownBy(() -> Payment.create(ORDER_ID, null, "USD"))
                .isInstanceOf(InvalidPaymentException.class);
        assertThatThrownBy(() -> Payment.create(ORDER_ID, BigDecimal.ZERO, "USD"))
                .isInstanceOf(InvalidPaymentException.class);
        assertThatThrownBy(() -> Payment.create(ORDER_ID, new BigDecimal("-1.00"), "USD"))
                .isInstanceOf(InvalidPaymentException.class);
    }

    @Test
    void createRejectsAmountsThatCannotBeRepresentedByThePaymentContract() {
        assertThatThrownBy(() -> Payment.create(ORDER_ID, new BigDecimal("1.001"), "USD"))
                .isInstanceOf(InvalidPaymentException.class)
                .hasMessageContaining("decimal places");
        assertThatThrownBy(() -> Payment.create(ORDER_ID, new BigDecimal("10000000000"), "USD"))
                .isInstanceOf(InvalidPaymentException.class)
                .hasMessageContaining("NUMERIC(12, 2)");
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = { "usd", "USDD", "US", "US D" })
    void createRejectsInvalidCurrency(String currency) {
        assertThatThrownBy(() -> Payment.create(ORDER_ID, AMOUNT, currency))
                .isInstanceOf(InvalidPaymentException.class)
                .hasMessageContaining("currency");
    }

    @Test
    void pendingPaymentCanCompleteWithProviderReference() {
        Payment payment = Payment.create(ORDER_ID, AMOUNT, "USD");

        payment.complete("simulated-payment-123");

        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.COMPLETED);
        assertThat(payment.getProviderReference()).isEqualTo("simulated-payment-123");
        assertThat(payment.getFailureReason()).isNull();
    }

    @Test
    void pendingPaymentCanFailWithReason() {
        Payment payment = Payment.create(ORDER_ID, AMOUNT, "USD");

        payment.fail("amount exceeds simulated provider limit");

        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.FAILED);
        assertThat(payment.getFailureReason()).isEqualTo("amount exceeds simulated provider limit");
        assertThat(payment.getProviderReference()).isNull();
    }

    @Test
    void finalPaymentCannotTransitionToAnotherOutcome() {
        Payment completedPayment = Payment.create(ORDER_ID, AMOUNT, "USD");
        completedPayment.complete("simulated-payment-123");

        assertThatThrownBy(() -> completedPayment.fail("declined"))
                .isInstanceOf(InvalidPaymentTransitionException.class);

        Payment failedPayment = Payment.create(ORDER_ID, AMOUNT, "USD");
        failedPayment.fail("declined");

        assertThatThrownBy(() -> failedPayment.complete("simulated-payment-456"))
                .isInstanceOf(InvalidPaymentTransitionException.class);
    }
}
