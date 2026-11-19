package com.eventforge.payment.kafka;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

import com.eventforge.contracts.order.OrderCreated;
import com.eventforge.payment.domain.InvalidPaymentException;
import com.eventforge.payment.domain.InvalidPaymentTransitionException;

@Configuration(proxyBeanMethods = false)
public class PaymentKafkaConfiguration {

    static final String RETRY_COUNT_HEADER = "retryCount";

    private static final String CONSUMER_NAME = "payment-service";
    private static final int IN_PLACE_RETRIES = 2;

    // Deterministic failures: retrying can never succeed, so they go straight to the DLQ.
    private static final List<Class<? extends Exception>> PERMANENT_ERRORS = List.of(
            IllegalArgumentException.class,
            DeserializationException.class,
            ClassCastException.class,
            InvalidPaymentException.class,
            InvalidPaymentTransitionException.class);

    /**
     * Main listener: a few quick in-place retries for transient errors, then the record moves to the retry topic so
     * the orders partition keeps flowing. Permanent errors skip the retries and go to the DLQ.
     */
    @Bean
    CommonErrorHandler paymentConsumerErrorHandler(
            KafkaTemplate<String, Object> template,
            @Value("${eventforge.payment.consumer.retry-topic}") String retryTopic,
            @Value("${eventforge.payment.consumer.dlq-topic}") String dlqTopic,
            @Value("${eventforge.payment.consumer.retry-backoff-ms}") long retryBackoffMs) {
        DeadLetterPublishingRecoverer recoverer = recoverer(
                template, (record, e) -> new TopicPartition(isPermanent(e) ? dlqTopic : retryTopic, -1));
        recoverer.setHeadersFunction((record, e) -> {
            Headers headers = new RecordHeaders(record.headers().toArray());
            headers.add("consumer", CONSUMER_NAME.getBytes(StandardCharsets.UTF_8));
            if (record.value() instanceof OrderCreated orderCreated) {
                headers.add("eventId", orderCreated.getEventId().toString().getBytes(StandardCharsets.UTF_8));
            }
            if (!isPermanent(e)) {
                headers.add(RETRY_COUNT_HEADER, "1".getBytes(StandardCharsets.UTF_8));
            }
            return headers;
        });
        return errorHandler(recoverer, new FixedBackOff(retryBackoffMs, IN_PLACE_RETRIES));
    }

    /** Retry listener: bounded exponential backoff; whatever still fails is dead-lettered. */
    @Bean
    ConcurrentKafkaListenerContainerFactory<Object, Object> paymentRetryListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> consumerFactory,
            KafkaTemplate<String, Object> template,
            @Value("${eventforge.payment.consumer.dlq-topic}") String dlqTopic,
            @Value("${eventforge.payment.consumer.retry-backoff-ms}") long retryBackoffMs,
            @Value("${eventforge.payment.consumer.retry-max-attempts}") int retryMaxAttempts) {
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(retryMaxAttempts - 1);
        backOff.setInitialInterval(retryBackoffMs);
        backOff.setMultiplier(2.0);
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, consumerFactory);
        factory.setCommonErrorHandler(errorHandler(
                recoverer(template, (record, e) -> new TopicPartition(dlqTopic, -1)), backOff));
        return factory;
    }

    private static DefaultErrorHandler errorHandler(
            DeadLetterPublishingRecoverer recoverer, BackOff backOff) {
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        PERMANENT_ERRORS.forEach(errorHandler::addNotRetryableExceptions);
        // manual acks: without this the recovered record's offset is only committed by the next successful ack
        errorHandler.setCommitRecovered(true);
        return errorHandler;
    }

    // Records that failed to deserialize are republished as their original bytes, so they need a byte[] template.
    private static DeadLetterPublishingRecoverer recoverer(
            KafkaTemplate<String, Object> template,
            BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> destination) {
        KafkaTemplate<String, Object> rawTemplate = new KafkaTemplate<>(
                template.getProducerFactory(),
                Map.of(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName()));
        Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
        templates.put(byte[].class, rawTemplate);
        templates.put(Object.class, template);
        return new DeadLetterPublishingRecoverer(templates, destination);
    }

    private static boolean isPermanent(Throwable e) {
        return e != null
                && (PERMANENT_ERRORS.stream().anyMatch(type -> type.isInstance(e)) || isPermanent(e.getCause()));
    }
}
