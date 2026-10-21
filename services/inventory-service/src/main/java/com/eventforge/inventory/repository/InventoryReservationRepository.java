package com.eventforge.inventory.repository;

import java.util.List;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;

import com.eventforge.inventory.domain.InventoryReservation;

public interface InventoryReservationRepository extends JpaRepository<InventoryReservation, UUID> {

    List<InventoryReservation> findAllByOrderId(UUID orderId);
}
