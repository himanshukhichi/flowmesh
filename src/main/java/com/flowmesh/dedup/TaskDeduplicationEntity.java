package com.flowmesh.dedup;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "task_deduplication")
public class TaskDeduplicationEntity {
    @Id
    @Column(name = "idempotency_key", length = 320)
    private String idempotencyKey;

    @Column(name = "dag_run_id", nullable = false)
    private UUID dagRunId;

    @Column(name = "task_id", nullable = false, length = 160)
    private String taskId;

    @Column(name = "worker_id", length = 160)
    private String workerId;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    protected TaskDeduplicationEntity() {
    }

    private TaskDeduplicationEntity(String idempotencyKey, UUID dagRunId, String taskId, String workerId) {
        this.idempotencyKey = idempotencyKey;
        this.dagRunId = dagRunId;
        this.taskId = taskId;
        this.workerId = workerId;
    }

    public static TaskDeduplicationEntity create(String idempotencyKey, UUID dagRunId, String taskId, String workerId) {
        return new TaskDeduplicationEntity(idempotencyKey, dagRunId, taskId, workerId);
    }

    @PrePersist
    void prePersist() {
        if (createdAt == null) {
            createdAt = Instant.now();
        }
    }
}
