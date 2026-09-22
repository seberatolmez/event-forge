package com.eventforge.order.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.eventforge.order.domain.InvalidOrderException;
import com.eventforge.order.domain.Order;
import com.eventforge.order.domain.OrderStatus;
import com.eventforge.order.repository.OrderRepository;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    private static final UUID CUSTOMER_ID = UUID.randomUUID();

    @Mock
    private OrderRepository orderRepository;

    private OrderService orderService;

    @BeforeEach
    void setUp() {
        orderService = new OrderService(orderRepository);
    }

    @Test
    void createOrderPersistsNewPendingOrder() {
        when(orderRepository.save(any(Order.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        Order order = orderService.createOrder(CUSTOMER_ID, new BigDecimal("49.50"), "EUR");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.PENDING);
        assertThat(order.getCustomerId()).isEqualTo(CUSTOMER_ID);
        assertThat(order.getTotalAmount()).isEqualByComparingTo("49.50");
        verify(orderRepository).save(any(Order.class));
    }

    @Test
    void createOrderRejectsInvalidOrderWithoutTouchingRepository() {
        assertThatThrownBy(() -> orderService.createOrder(CUSTOMER_ID, BigDecimal.ZERO, "USD"))
                .isInstanceOf(InvalidOrderException.class);

        verify(orderRepository, never()).save(any(Order.class));
    }

    @Test
    void getOrderReturnsExistingOrder() {
        Order stored = Order.create(CUSTOMER_ID, new BigDecimal("10.00"), "USD");
        UUID storedId = UUID.randomUUID();
        when(orderRepository.findById(storedId)).thenReturn(Optional.of(stored));

        Order result = orderService.getOrder(storedId);

        assertThat(result).isSameAs(stored);
    }

    @Test
    void getOrderThrowsWhenOrderDoesNotExist() {
        UUID unknownId = UUID.randomUUID();
        when(orderRepository.findById(unknownId)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> orderService.getOrder(unknownId))
                .isInstanceOf(OrderNotFoundException.class)
                .hasMessageContaining(unknownId.toString());
    }
}
