package com.eventforge.order.outbox;

import java.util.List;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    List<OutboxEvent> findAllByAggregateId(UUID aggregateId);

    @Query(value = """
            SELECT *
            FROM order_service.outbox_events
            WHERE published_at IS NULL
            ORDER BY created_at, id
            LIMIT :batchSize
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<OutboxEvent> findUnpublishedForUpdate(@Param("batchSize") int batchSize);
}
