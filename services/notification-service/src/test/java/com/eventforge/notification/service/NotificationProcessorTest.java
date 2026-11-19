package com.eventforge.notification.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.DuplicateKeyException;

import com.eventforge.contracts.inventory.InventoryReservationFailed;
import com.eventforge.contracts.inventory.InventoryReservationFailedPayload;
import com.eventforge.contracts.order.OrderCreated;
import com.eventforge.contracts.order.OrderCreatedPayload;
import com.eventforge.contracts.payment.PaymentFailed;
import com.eventforge.contracts.payment.PaymentFailedPayload;
import com.eventforge.notification.domain.Notification;
import com.eventforge.notification.domain.NotificationStatus;
import com.eventforge.notification.provider.MockNotificationProvider;
import com.eventforge.notification.repository.NotificationRepository;

class NotificationProcessorTest {

    private final NotificationRepository notificationRepository = mock(NotificationRepository.class);
    private final MockNotificationProvider provider = mock(MockNotificationProvider.class);
    private final NotificationProcessor processor =
            new NotificationProcessor(notificationRepository, provider);

    @Test
    void orderCreatedSendsNotificationToTheCustomer() {
        UUID orderId = UUID.randomUUID();
        UUID customerId = UUID.randomUUID();
        when(notificationRepository.saveAndFlush(any(Notification.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        OrderCreated orderCreated = OrderCreated.newBuilder()
                .setEventId(UUID.randomUUID())
                .setEventType("OrderCreated")
                .setEventVersion(2)
                .setOccurredAt(Instant.parse("2026-10-27T10:00:00.123Z"))
                .setAggregateType("Order")
                .setAggregateId(orderId)
                .setPayload(OrderCreatedPayload.newBuilder()
                        .setOrderId(orderId)
                        .setCustomerId(customerId)
                        .setTotalAmount(new BigDecimal("99.00"))
                        .setCurrency("USD")
                        .build())
                .build();

        processor.processOrderCreated(orderCreated);

        ArgumentCaptor<Notification> captor = ArgumentCaptor.forClass(Notification.class);
        verify(notificationRepository).saveAndFlush(captor.capture());
        verify(provider, times(1)).send(any(Notification.class));
        Notification notification = captor.getValue();
        assertThat(notification.getEventId()).isEqualTo(orderCreated.getEventId());
        assertThat(notification.getType()).isEqualTo("OrderCreated");
        assertThat(notification.getRecipient()).isEqualTo(customerId.toString());
        assertThat(notification.getStatus()).isEqualTo(NotificationStatus.SENT);
    }

    @Test
    void paymentFailedSendsNotificationToTheOrder() {
        when(notificationRepository.saveAndFlush(any(Notification.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        PaymentFailed paymentFailed = PaymentFailed.newBuilder()
                .setEventId(UUID.randomUUID())
                .setEventType("PaymentFailed")
                .setEventVersion(1)
                .setOccurredAt(Instant.parse("2026-10-27T10:00:01.123Z"))
                .setAggregateType("Payment")
                .setAggregateId(UUID.randomUUID())
                .setPayload(PaymentFailedPayload.newBuilder()
                        .setPaymentId(UUID.randomUUID())
                        .setOrderId(UUID.randomUUID())
                        .setReason("amount exceeds simulated provider limit")
                        .build())
                .build();

        processor.processPaymentFailed(paymentFailed);

        ArgumentCaptor<Notification> captor = ArgumentCaptor.forClass(Notification.class);
        verify(notificationRepository).saveAndFlush(captor.capture());
        Notification notification = captor.getValue();
        assertThat(notification.getRecipient()).isEqualTo(
                paymentFailed.getPayload().getOrderId().toString());
        assertThat(notification.getType()).isEqualTo("PaymentFailed");
        assertThat(notification.getStatus()).isEqualTo(NotificationStatus.SENT);
    }

    @Test
    void inventoryReservationFailedSendsNotificationToTheOrder() {
        when(notificationRepository.saveAndFlush(any(Notification.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        InventoryReservationFailed failed = InventoryReservationFailed.newBuilder()
                .setEventId(UUID.randomUUID())
                .setEventType("InventoryReservationFailed")
                .setEventVersion(1)
                .setOccurredAt(Instant.parse("2026-10-27T10:00:02.123Z"))
                .setAggregateType("Inventory")
                .setAggregateId(UUID.randomUUID())
                .setPayload(InventoryReservationFailedPayload.newBuilder()
                        .setOrderId(UUID.randomUUID())
                        .setProductId(UUID.randomUUID())
                        .setQuantity(2)
                        .setReason("insufficient stock")
                        .build())
                .build();

        processor.processInventoryReservationFailed(failed);

        ArgumentCaptor<Notification> captor = ArgumentCaptor.forClass(Notification.class);
        verify(notificationRepository).saveAndFlush(captor.capture());
        assertThat(captor.getValue().getRecipient()).isEqualTo(
                failed.getPayload().getOrderId().toString());
        assertThat(captor.getValue().getType()).isEqualTo("InventoryReservationFailed");
    }

    @Test
    void duplicateEventIdIsRejectedByTheUniqueConstraint() {
        when(notificationRepository.saveAndFlush(any(Notification.class)))
                .thenThrow(new DuplicateKeyException("unique_violation"));
        PaymentFailed paymentFailed = PaymentFailed.newBuilder()
                .setEventId(UUID.randomUUID())
                .setEventType("PaymentFailed")
                .setEventVersion(1)
                .setOccurredAt(Instant.now())
                .setAggregateType("Payment")
                .setAggregateId(UUID.randomUUID())
                .setPayload(PaymentFailedPayload.newBuilder()
                        .setPaymentId(UUID.randomUUID())
                        .setOrderId(UUID.randomUUID())
                        .setReason("declined")
                        .build())
                .build();

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> processor.processPaymentFailed(paymentFailed))
                .isInstanceOf(DuplicateKeyException.class);
        verify(provider, times(0)).send(any(Notification.class));
    }
}
