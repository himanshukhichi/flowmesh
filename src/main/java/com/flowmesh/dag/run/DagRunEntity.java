package com.flowmesh.dag.run;

import com.flowmesh.dag.persistence.DagDefinitionEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "dag_runs")
public class DagRunEntity {
    @Id
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "dag_definition_id", nullable = false)
    private DagDefinitionEntity dagDefinition;

    @Column(name = "dag_id", nullable = false, length = 120)
    private String dagId;

    @Column(nullable = false)
    private int version;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 24)
    private DagRunStatus status;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "started_at")
    private Instant startedAt;

    @Column(name = "completed_at")
    private Instant completedAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected DagRunEntity() {
    }

    private DagRunEntity(DagDefinitionEntity dagDefinition) {
        this.id = UUID.randomUUID();
        this.dagDefinition = dagDefinition;
        this.dagId = dagDefinition.getDagId();
        this.version = dagDefinition.getVersion();
        this.status = DagRunStatus.CREATED;
    }

    public static DagRunEntity create(DagDefinitionEntity dagDefinition) {
        return new DagRunEntity(dagDefinition);
    }

    public void markRunning() {
        this.status = DagRunStatus.RUNNING;
        this.startedAt = Instant.now();
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

    public UUID getId() {
        return id;
    }

    public String getDagId() {
        return dagId;
    }

    public int getVersion() {
        return version;
    }

    public DagRunStatus getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public Instant getCompletedAt() {
        return completedAt;
    }
}
