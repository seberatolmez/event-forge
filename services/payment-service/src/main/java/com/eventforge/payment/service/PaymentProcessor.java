package com.eventforge.payment.service;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.eventforge.contracts.order.OrderCreated;
import com.eventforge.contracts.order.OrderCreatedPayload;
import com.eventforge.payment.dedup.ProcessedEvent;
import com.eventforge.payment.dedup.ProcessedEventRepository;
import com.eventforge.payment.domain.Payment;
import com.eventforge.payment.outbox.OutboxEvent;
import com.eventforge.payment.outbox.OutboxEventRepository;
import com.eventforge.payment.outbox.PaymentEventPayload;
import com.eventforge.payment.provider.PaymentProviderResult;
import com.eventforge.payment.provider.SimulatedPaymentProvider;
import com.eventforge.payment.repository.PaymentRepository;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentProcessor {

    private final PaymentRepository paymentRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final SimulatedPaymentProvider simulatedPaymentProvider;
    private final ObjectMapper objectMapper;

    @Transactional
    public boolean processOrderCreated(OrderCreated orderCreated) {
        if (processedEventRepository.existsById(orderCreated.getEventId())) {
            log.info("Ignoring already processed OrderCreated event {}", orderCreated.getEventId());
            return false;
        }

        OrderCreatedPayload order = orderCreated.getPayload();
        Payment payment = paymentRepository.save(Payment.create(
                order.getOrderId(), order.getTotalAmount(), order.getCurrency().toString()));
        PaymentProviderResult result = simulatedPaymentProvider.charge(payment);
        OutboxEvent event;
        if (result.approved()) {
            payment.complete(result.providerReference());
            event = OutboxEvent.create(
                    "Payment",
                    payment.getId(),
                    "PaymentCompleted",
                    1,
                    objectMapper.valueToTree(new PaymentEventPayload.Completed(
                            payment.getId(),
                            payment.getOrderId(),
                            payment.getAmount(),
                            payment.getCurrency(),
                            payment.getProviderReference())));
        } else {
            payment.fail(result.failureReason());
            event = OutboxEvent.create(
                    "Payment",
                    payment.getId(),
                    "PaymentFailed",
                    1,
                    objectMapper.valueToTree(new PaymentEventPayload.Failed(
                            payment.getId(),
                            payment.getOrderId(),
                            payment.getFailureReason())));
        }
        outboxEventRepository.save(event);
        processedEventRepository.save(ProcessedEvent.create(
                orderCreated.getEventId(),
                orderCreated.getEventType().toString(),
                orderCreated.getEventVersion()));
        return true;
    }
}
