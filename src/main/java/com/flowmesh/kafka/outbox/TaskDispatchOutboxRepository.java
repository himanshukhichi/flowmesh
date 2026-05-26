package com.flowmesh.kafka.outbox;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.UUID;

public interface TaskDispatchOutboxRepository extends JpaRepository<TaskDispatchOutboxEntity, UUID> {

    @Query(value = """
            select *
            from task_dispatch_outbox
            where state = 'PENDING'
              and (next_attempt_at is null or next_attempt_at <= now())
            order by created_at
            limit :limit
            for update skip locked
            """, nativeQuery = true)
    List<TaskDispatchOutboxEntity> lockPending(@Param("limit") int limit);
}
