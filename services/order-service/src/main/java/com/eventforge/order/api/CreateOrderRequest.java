package com.eventforge.order.api;

import java.math.BigDecimal;
import java.util.UUID;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

public record CreateOrderRequest(
        @NotNull UUID customerId,
        @NotNull @DecimalMin(value = "0.01", message = "totalAmount must be greater than zero") BigDecimal totalAmount,
        @NotBlank @Pattern(regexp = "[A-Z]{3}", message = "currency must be a 3-letter ISO 4217 code") String currency) {
}
