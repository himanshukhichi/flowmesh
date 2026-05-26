package com.flowmesh.worker.grpc;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;

@Entity
@Table(name = "worker_registrations")
public class WorkerRegistrationEntity {
    @Id
    @Column(name = "worker_id", length = 160)
    private String workerId;

    @Column(name = "supported_task_types", nullable = false, columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private String supportedTaskTypesJson;

    @Column(name = "max_concurrent", nullable = false)
    private int maxConcurrent;

    @Column(name = "current_task_id", length = 160)
    private String currentTaskId;

    @Column(nullable = false, length = 32)
    private String status;

    @Column(name = "last_heartbeat_at")
    private Instant lastHeartbeatAt;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected WorkerRegistrationEntity() {
    }

    private WorkerRegistrationEntity(String workerId, String supportedTaskTypesJson, int maxConcurrent) {
        this.workerId = workerId;
        this.supportedTaskTypesJson = supportedTaskTypesJson;
        this.maxConcurrent = maxConcurrent;
        this.status = "REGISTERED";
    }

    public static WorkerRegistrationEntity create(String workerId, String supportedTaskTypesJson, int maxConcurrent) {
        return new WorkerRegistrationEntity(workerId, supportedTaskTypesJson, maxConcurrent);
    }

    public void updateRegistration(String supportedTaskTypesJson, int maxConcurrent) {
        this.supportedTaskTypesJson = supportedTaskTypesJson;
        this.maxConcurrent = maxConcurrent;
        this.status = "REGISTERED";
    }

    public void heartbeat(String currentTaskId) {
        this.currentTaskId = currentTaskId;
        this.lastHeartbeatAt = Instant.now();
        this.status = currentTaskId == null || currentTaskId.isBlank() ? "IDLE" : "RUNNING";
    }

    @PrePersist
    void prePersist() {
        Instant now = Instant.now();
        if (createdAt == null) {
            createdAt = now;
        }
        if (updatedAt == null) {
            updatedAt = now;
        }
    }

    @PreUpdate
    void preUpdate() {
        updatedAt = Instant.now();
    }
}
