package com.eventforge.payment.provider;

public record PaymentProviderResult(String providerReference, String failureReason) {

    public static PaymentProviderResult approved(String providerReference) {
        return new PaymentProviderResult(providerReference, null);
    }

    public static PaymentProviderResult declined(String reason) {
        return new PaymentProviderResult(null, reason);
    }

    public boolean approved() {
        return providerReference != null;
    }
}
