package com.eventforge.inventory.kafka;

import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration(proxyBeanMethods = false)
public class InventoryKafkaConfiguration {

    @Bean
    CommonErrorHandler inventoryConsumerErrorHandler(
            @Value("${eventforge.inventory.consumer.retry-backoff-ms}") long retryBackoffMs) {
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                new FixedBackOff(retryBackoffMs, FixedBackOff.UNLIMITED_ATTEMPTS));
        errorHandler.setClassifications(Map.of(Exception.class, true), true);
        return errorHandler;
    }
}
