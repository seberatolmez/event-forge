package com.eventforge.payment.provider;

import java.math.BigDecimal;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.eventforge.payment.domain.Payment;

@Component
public class SimulatedPaymentProvider {

    private static final String DECLINE_REASON = "amount exceeds simulated provider limit";

    private final BigDecimal declineAbove;

    public SimulatedPaymentProvider(
            @Value("${eventforge.payment.simulator.decline-above}") BigDecimal declineAbove) {
        if (declineAbove == null || declineAbove.signum() <= 0) {
            throw new IllegalArgumentException("payment decline threshold must be greater than zero");
        }
        this.declineAbove = declineAbove;
    }

    public PaymentProviderResult charge(Payment payment) {
        if (payment.getAmount().compareTo(declineAbove) > 0) {
            return PaymentProviderResult.declined(DECLINE_REASON);
        }
        return PaymentProviderResult.approved("simulated-payment-" + payment.getOrderId());
    }
}
