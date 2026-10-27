package com.eventforge.inventory.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import com.eventforge.contracts.order.OrderCreated;
import com.eventforge.contracts.order.OrderCreatedPayload;
import com.eventforge.contracts.order.OrderItemPayload;
import com.eventforge.inventory.dedup.ProcessedEvent;
import com.eventforge.inventory.dedup.ProcessedEventRepository;
import com.eventforge.inventory.domain.InventoryReservation;
import com.eventforge.inventory.domain.Product;
import com.eventforge.inventory.outbox.OutboxEvent;
import com.eventforge.inventory.outbox.OutboxEventRepository;
import com.eventforge.inventory.repository.InventoryReservationRepository;
import com.eventforge.inventory.repository.ProductRepository;
import com.fasterxml.jackson.databind.ObjectMapper;

@ExtendWith(MockitoExtension.class)
class InventoryProcessorTest {

    private static final UUID ORDER_ID = UUID.randomUUID();
    private static final UUID EVENT_ID = UUID.randomUUID();

    @Mock
    private ProductRepository productRepository;

    @Mock
    private InventoryReservationRepository reservationRepository;

    @Mock
    private OutboxEventRepository outboxEventRepository;

    @Mock
    private ProcessedEventRepository processedEventRepository;

    private InventoryProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new InventoryProcessor(
                productRepository,
                reservationRepository,
                outboxEventRepository,
                processedEventRepository,
                new ObjectMapper());
    }

    @Test
    void reservesEveryItemAndStoresMatchingOutboxEvents() {
        UUID productId = UUID.randomUUID();
        Product product = product(productId, 5);
        when(processedEventRepository.existsById(EVENT_ID)).thenReturn(false);
        when(productRepository.findAllForUpdate(List.of(productId))).thenReturn(List.of(product));
        when(reservationRepository.save(any(InventoryReservation.class))).thenAnswer(invocation -> {
            InventoryReservation saved = invocation.getArgument(0);
            ReflectionTestUtils.setField(saved, "id", UUID.randomUUID());
            return saved;
        });

        boolean processed = processor.processOrderCreated(event(2, item(productId, 2)));

        assertThat(processed).isTrue();
        assertThat(product.getAvailableQuantity()).isEqualTo(3);
        assertThat(product.getReservedQuantity()).isEqualTo(2);
        verify(reservationRepository).save(any(InventoryReservation.class));
        ArgumentCaptor<OutboxEvent> eventCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(eventCaptor.capture());
        assertThat(eventCaptor.getValue().getEventType()).isEqualTo("InventoryReserved");
        assertThat(eventCaptor.getValue().getPayload().path("orderId").asText()).isEqualTo(ORDER_ID.toString());
        verify(processedEventRepository).save(any(ProcessedEvent.class));
    }

    @Test
    void insufficientStockEmitsFailureWithoutReservingAnyItems() {
        UUID productId = UUID.randomUUID();
        Product product = product(productId, 1);
        when(processedEventRepository.existsById(EVENT_ID)).thenReturn(false);
        when(productRepository.findAllForUpdate(List.of(productId))).thenReturn(List.of(product));

        processor.processOrderCreated(event(2, item(productId, 2)));

        assertThat(product.getAvailableQuantity()).isEqualTo(1);
        assertThat(product.getReservedQuantity()).isZero();
        verify(reservationRepository, never()).save(any(InventoryReservation.class));
        ArgumentCaptor<OutboxEvent> eventCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(eventCaptor.capture());
        assertThat(eventCaptor.getValue().getEventType()).isEqualTo("InventoryReservationFailed");
        assertThat(eventCaptor.getValue().getPayload().path("reason").asText()).contains("insufficient stock");
    }

    @Test
    void failureOnOneItemLeavesOtherProductsUntouched() {
        UUID availableId = UUID.randomUUID();
        UUID unavailableId = UUID.randomUUID();
        Product available = product(availableId, 5);
        Product unavailable = product(unavailableId, 1);
        when(processedEventRepository.existsById(EVENT_ID)).thenReturn(false);
        when(productRepository.findAllForUpdate(anyList())).thenReturn(List.of(available, unavailable));

        processor.processOrderCreated(event(2, item(availableId, 2), item(unavailableId, 2)));

        assertThat(available.getAvailableQuantity()).isEqualTo(5);
        assertThat(available.getReservedQuantity()).isZero();
        assertThat(unavailable.getAvailableQuantity()).isEqualTo(1);
        assertThat(unavailable.getReservedQuantity()).isZero();
        verify(reservationRepository, never()).save(any(InventoryReservation.class));
        ArgumentCaptor<OutboxEvent> eventCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(eventCaptor.capture());
        assertThat(eventCaptor.getValue().getEventType()).isEqualTo("InventoryReservationFailed");
    }

    @Test
    void duplicateEventDoesNotLoadProductsOrCreateSideEffects() {
        when(processedEventRepository.existsById(EVENT_ID)).thenReturn(true);

        assertThat(processor.processOrderCreated(event(2, item(UUID.randomUUID(), 1)))).isFalse();

        verifyNoInteractions(productRepository, reservationRepository, outboxEventRepository);
        verify(processedEventRepository, never()).save(any(ProcessedEvent.class));
    }

    @Test
    void legacyEventWithoutItemsIsRecordedWithoutInventoryChanges() {
        when(processedEventRepository.existsById(EVENT_ID)).thenReturn(false);

        assertThat(processor.processOrderCreated(event(1))).isTrue();

        verifyNoInteractions(productRepository, reservationRepository, outboxEventRepository);
        verify(processedEventRepository).save(any(ProcessedEvent.class));
    }

    @Test
    void versionTwoEventWithoutItemsIsRejected() {
        when(processedEventRepository.existsById(EVENT_ID)).thenReturn(false);

        assertThatThrownBy(() -> processor.processOrderCreated(event(2)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one item");
        verifyNoInteractions(productRepository, reservationRepository, outboxEventRepository);
    }

    private static Product product(UUID id, int available) {
        Product product = Product.create("SKU-" + id, "Test product", available);
        ReflectionTestUtils.setField(product, "id", id);
        return product;
    }

    private static OrderItemPayload item(UUID productId, int quantity) {
        return OrderItemPayload.newBuilder().setProductId(productId).setQuantity(quantity).build();
    }

    private static OrderCreated event(int version, OrderItemPayload... items) {
        return OrderCreated.newBuilder()
                .setEventId(EVENT_ID)
                .setEventType("OrderCreated")
                .setEventVersion(version)
                .setOccurredAt(Instant.now())
                .setAggregateType("Order")
                .setAggregateId(ORDER_ID)
                .setPayload(OrderCreatedPayload.newBuilder()
                        .setOrderId(ORDER_ID)
                        .setCustomerId(UUID.randomUUID())
                        .setTotalAmount(new java.math.BigDecimal("10.00"))
                        .setCurrency("USD")
                        .setItems(List.of(items))
                        .build())
                .build();
    }
}
