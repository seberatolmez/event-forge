package com.eventforge.order.service;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.eventforge.order.domain.Order;
import com.eventforge.order.domain.OrderItem;
import com.eventforge.order.outbox.OrderCreatedPayload;
import com.eventforge.order.outbox.OutboxEvent;
import com.eventforge.order.outbox.OutboxEventRepository;
import com.eventforge.order.repository.OrderRepository;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    @Transactional
    public Order createOrder(UUID customerId, BigDecimal totalAmount, String currency, List<OrderItem> items) {
        Order order = orderRepository.save(Order.create(customerId, totalAmount, currency, items));
        OrderCreatedPayload payload = new OrderCreatedPayload(
                order.getId(),
                order.getCustomerId(),
                order.getTotalAmount(),
                order.getCurrency(),
                order.getItems().stream()
                        .map(item -> new OrderCreatedPayload.Item(item.getProductId(), item.getQuantity()))
                        .toList());
        OutboxEvent event = OutboxEvent.create(
                "Order",
                order.getId(),
                "OrderCreated",
                2,
                objectMapper.valueToTree(payload));
        outboxEventRepository.save(event);
        return order;
    }

    @Transactional(readOnly = true)
    public Order getOrder(UUID orderId) {
        return orderRepository.findWithItemsById(orderId)
                .orElseThrow(() -> new OrderNotFoundException("order %s not found".formatted(orderId)));
    }
}
