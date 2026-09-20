package com.eventforge.order.repository;

import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;

import com.eventforge.order.domain.Order;

public interface OrderRepository extends JpaRepository<Order, UUID> {
}
