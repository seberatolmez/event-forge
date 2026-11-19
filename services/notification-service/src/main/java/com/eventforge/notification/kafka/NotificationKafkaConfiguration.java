package com.eventforge.notification.kafka;

import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.FixedBackOff;

import lombok.extern.slf4j.Slf4j;

@Configuration(proxyBeanMethods = false)
@Slf4j
public class NotificationKafkaConfiguration {

    @Bean
    CommonErrorHandler notificationConsumerErrorHandler(
            @Value("${eventforge.notification.consumer.retry-backoff-ms}") long retryBackoffMs) {
        // Records this service cannot deserialize (e.g. InventoryReserved, which it does not consume) are skipped.
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                (record, e) -> log.warn("Skipping undeserializable record {}-{}@{}: {}",
                        record.topic(), record.partition(), record.offset(), e.getMessage()),
                new FixedBackOff(retryBackoffMs, FixedBackOff.UNLIMITED_ATTEMPTS));
        errorHandler.setClassifications(
                Map.of(Exception.class, true, DeserializationException.class, false), true);
        return errorHandler;
    }
}
