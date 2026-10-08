package com.eventforge.order.service;

import java.math.BigDecimal;
import java.util.UUID;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.eventforge.order.domain.Order;
import com.eventforge.order.outbox.OrderCreatedPayload;
import com.eventforge.order.outbox.OutboxEvent;
import com.eventforge.order.outbox.OutboxEventRepository;
import com.eventforge.order.repository.OrderRepository;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public OrderService(
            OrderRepository orderRepository,
            OutboxEventRepository outboxEventRepository,
            ObjectMapper objectMapper) {
        this.orderRepository = orderRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public Order createOrder(UUID customerId, BigDecimal totalAmount, String currency) {
        Order order = orderRepository.save(Order.create(customerId, totalAmount, currency));
        OrderCreatedPayload payload = new OrderCreatedPayload(
                order.getId(), order.getCustomerId(), order.getTotalAmount(), order.getCurrency());
        OutboxEvent event = OutboxEvent.create(
                "Order",
                order.getId(),
                "OrderCreated",
                1,
                objectMapper.valueToTree(payload));
        outboxEventRepository.save(event);
        return order;
    }

    @Transactional(readOnly = true)
    public Order getOrder(UUID orderId) {
        return orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException("order %s not found".formatted(orderId)));
    }
}
