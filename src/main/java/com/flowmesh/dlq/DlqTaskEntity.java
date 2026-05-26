package com.flowmesh.dlq;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "dlq_tasks")
public class DlqTaskEntity {
    @Id
    @GeneratedValue
    private UUID id;

    @Column(name = "dag_run_id", nullable = false)
    private UUID dagRunId;

    @Column(name = "task_id", nullable = false, length = 160)
    private String taskId;

    @Column(name = "task_type", nullable = false, length = 120)
    private String taskType;

    @Column(name = "idempotency_key", nullable = false, length = 320)
    private String idempotencyKey;

    @Column(name = "payload_json", nullable = false, columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private String payloadJson;

    @Column(name = "error_message")
    private String errorMessage;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "requeued_at")
    private Instant requeuedAt;

    protected DlqTaskEntity() {
    }

    private DlqTaskEntity(UUID dagRunId, String taskId, String taskType, String idempotencyKey, String payloadJson, String errorMessage) {
        this.dagRunId = dagRunId;
        this.taskId = taskId;
        this.taskType = taskType;
        this.idempotencyKey = idempotencyKey;
        this.payloadJson = payloadJson;
        this.errorMessage = errorMessage;
    }

    public static DlqTaskEntity create(
            UUID dagRunId,
            String taskId,
            String taskType,
            String idempotencyKey,
            String payloadJson,
            String errorMessage
    ) {
        return new DlqTaskEntity(dagRunId, taskId, taskType, idempotencyKey, payloadJson, errorMessage);
    }

    public void markRequeued() {
        this.requeuedAt = Instant.now();
    }

    @PrePersist
    void prePersist() {
        if (createdAt == null) {
            createdAt = Instant.now();
        }
    }

    public UUID getId() {
        return id;
    }

    public UUID getDagRunId() {
        return dagRunId;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getTaskType() {
        return taskType;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public String getPayloadJson() {
        return payloadJson;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getRequeuedAt() {
        return requeuedAt;
    }
}
