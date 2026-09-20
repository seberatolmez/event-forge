package com.eventforge.order.service;

import java.math.BigDecimal;
import java.util.UUID;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.eventforge.order.domain.Order;
import com.eventforge.order.repository.OrderRepository;

@Service
public class OrderService {

    private final OrderRepository orderRepository;

    public OrderService(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    @Transactional
    public Order createOrder(UUID customerId, BigDecimal totalAmount, String currency) {
        Order order = Order.create(customerId, totalAmount, currency);
        return orderRepository.save(order);
    }

    @Transactional(readOnly = true)
    public Order getOrder(UUID orderId) {
        return orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException("order %s not found".formatted(orderId)));
    }
}
