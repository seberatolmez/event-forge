package com.eventforge.inventory.service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.eventforge.contracts.order.OrderCreated;
import com.eventforge.contracts.order.OrderCreatedPayload;
import com.eventforge.contracts.order.OrderItemPayload;
import com.eventforge.inventory.dedup.ProcessedEvent;
import com.eventforge.inventory.dedup.ProcessedEventRepository;
import com.eventforge.inventory.domain.InventoryEventPayload;
import com.eventforge.inventory.domain.InventoryReservation;
import com.eventforge.inventory.domain.Product;
import com.eventforge.inventory.outbox.OutboxEvent;
import com.eventforge.inventory.outbox.OutboxEventRepository;
import com.eventforge.inventory.repository.InventoryReservationRepository;
import com.eventforge.inventory.repository.ProductRepository;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
@RequiredArgsConstructor
@Slf4j
public class InventoryProcessor {

    private final ProductRepository productRepository;
    private final InventoryReservationRepository reservationRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final ObjectMapper objectMapper;

    @Transactional
    public boolean processOrderCreated(OrderCreated orderCreated) {
        if (orderCreated == null || orderCreated.getEventId() == null || orderCreated.getPayload() == null) {
            throw new IllegalArgumentException("OrderCreated event metadata and payload are required");
        }
        if (processedEventRepository.existsById(orderCreated.getEventId())) {
            log.info("Ignoring already processed OrderCreated event {}", orderCreated.getEventId());
            return false;
        }

        OrderCreatedPayload payload = orderCreated.getPayload();
        UUID orderId = payload.getOrderId();
        if (orderId == null || !orderId.equals(orderCreated.getAggregateId())) {
            throw new IllegalArgumentException("OrderCreated aggregateId must match payload orderId");
        }

        List<RequestedItem> items = requestedItems(payload.getItems());
        if (items.isEmpty()) {
            if (orderCreated.getEventVersion() != 1) {
                throw new IllegalArgumentException("OrderCreated version 2 must contain at least one item");
            }
            log.info("Ignoring legacy OrderCreated event {} with no item data", orderCreated.getEventId());
            markProcessed(orderCreated);
            return true;
        }

        List<UUID> productIds = items.stream().map(RequestedItem::productId).sorted().toList();
        Map<UUID, Product> products = productRepository.findAllForUpdate(productIds).stream()
                .collect(Collectors.toMap(Product::getId, Function.identity()));

        RequestedItem failedItem = items.stream()
                .filter(item -> !products.containsKey(item.productId())
                        || products.get(item.productId()).getAvailableQuantity() < item.quantity())
                .findFirst()
                .orElse(null);

        if (failedItem != null) {
            Product product = products.get(failedItem.productId());
            String reason = product == null
                    ? "product not found"
                    : "insufficient stock: available %d, requested %d"
                            .formatted(product.getAvailableQuantity(), failedItem.quantity());
            outboxEventRepository.save(OutboxEvent.create(
                    "Order",
                    orderId,
                    "InventoryReservationFailed",
                    1,
                    objectMapper.valueToTree(new InventoryEventPayload.Failed(
                            orderId, failedItem.productId(), failedItem.quantity(), reason))));
        } else {
            for (RequestedItem item : items) {
                products.get(item.productId()).reserve(item.quantity());
                InventoryReservation reservation = reservationRepository.save(
                        InventoryReservation.create(orderId, item.productId(), item.quantity()));
                outboxEventRepository.save(OutboxEvent.create(
                        "Order",
                        orderId,
                        "InventoryReserved",
                        1,
                        objectMapper.valueToTree(new InventoryEventPayload.Reserved(
                                reservation.getId(), orderId, item.productId(), item.quantity()))));
            }
        }

        markProcessed(orderCreated);
        return true;
    }

    private List<RequestedItem> requestedItems(List<OrderItemPayload> payloadItems) {
        if (payloadItems == null || payloadItems.isEmpty()) {
            return List.of();
        }
        Set<UUID> productIds = new HashSet<>();
        List<RequestedItem> items = new ArrayList<>(payloadItems.size());
        for (OrderItemPayload item : payloadItems) {
            if (item == null || item.getProductId() == null || item.getQuantity() < 1) {
                throw new IllegalArgumentException("OrderCreated items require a productId and positive quantity");
            }
            if (!productIds.add(item.getProductId())) {
                throw new IllegalArgumentException("OrderCreated items must not repeat a productId");
            }
            items.add(new RequestedItem(item.getProductId(), item.getQuantity()));
        }
        return List.copyOf(items);
    }

    private void markProcessed(OrderCreated orderCreated) {
        processedEventRepository.save(ProcessedEvent.create(
                orderCreated.getEventId(),
                orderCreated.getEventType().toString(),
                orderCreated.getEventVersion()));
    }

    private record RequestedItem(UUID productId, int quantity) {
    }
}
