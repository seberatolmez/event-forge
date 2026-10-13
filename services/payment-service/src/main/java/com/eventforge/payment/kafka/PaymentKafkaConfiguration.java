package com.eventforge.payment.kafka;

import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration(proxyBeanMethods = false)
public class PaymentKafkaConfiguration {

    @Bean
    CommonErrorHandler paymentConsumerErrorHandler(
            @Value("${eventforge.payment.consumer.retry-backoff-ms:1000}") long retryBackoffMs) {
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                new FixedBackOff(retryBackoffMs, FixedBackOff.UNLIMITED_ATTEMPTS));
        errorHandler.setClassifications(Map.of(Exception.class, true), true);
        return errorHandler;
    }
}
