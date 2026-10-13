package com.eventforge.payment.provider;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.eventforge.payment.domain.Payment;

class SimulatedPaymentProviderTest {

    private final SimulatedPaymentProvider provider = new SimulatedPaymentProvider(new BigDecimal("1000.00"));

    @Test
    void approvesAmountsAtOrBelowTheConfiguredThreshold() {
        UUID orderId = UUID.randomUUID();
        Payment payment = Payment.create(orderId, new BigDecimal("1000.00"), "USD");

        PaymentProviderResult result = provider.charge(payment);

        assertThat(result.approved()).isTrue();
        assertThat(result.providerReference()).isEqualTo("simulated-payment-" + orderId);
        assertThat(result.failureReason()).isNull();
    }

    @Test
    void declinesAmountsAboveTheConfiguredThreshold() {
        Payment payment = Payment.create(UUID.randomUUID(), new BigDecimal("1000.01"), "USD");

        PaymentProviderResult result = provider.charge(payment);

        assertThat(result.approved()).isFalse();
        assertThat(result.providerReference()).isNull();
        assertThat(result.failureReason()).isEqualTo("amount exceeds simulated provider limit");
    }
}
