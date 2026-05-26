package com.flowmesh.timeline;

import com.flowmesh.dag.run.TaskRunEntity;
import com.flowmesh.dag.run.TaskState;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "task_state_transitions")
public class TaskStateTransitionEntity {
    @Id
    @GeneratedValue
    private UUID id;

    @Column(name = "dag_run_id", nullable = false)
    private UUID dagRunId;

    @ManyToOne(optional = false)
    @JoinColumn(name = "task_run_id", nullable = false)
    private TaskRunEntity taskRun;

    @Column(name = "task_id", nullable = false, length = 160)
    private String taskId;

    @Enumerated(EnumType.STRING)
    @Column(name = "from_state", length = 24)
    private TaskState fromState;

    @Enumerated(EnumType.STRING)
    @Column(name = "to_state", nullable = false, length = 24)
    private TaskState toState;

    @Column(length = 240)
    private String reason;

    @Column(name = "details_json", nullable = false, columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private String detailsJson;

    @Column(name = "transitioned_at", nullable = false, updatable = false)
    private Instant transitionedAt;

    protected TaskStateTransitionEntity() {
    }

    private TaskStateTransitionEntity(TaskRunEntity taskRun, TaskState fromState, TaskState toState, String reason, String detailsJson) {
        this.dagRunId = taskRun.getDagRun().getId();
        this.taskRun = taskRun;
        this.taskId = taskRun.getTaskId();
        this.fromState = fromState;
        this.toState = toState;
        this.reason = reason;
        this.detailsJson = detailsJson == null ? "{}" : detailsJson;
    }

    public static TaskStateTransitionEntity create(
            TaskRunEntity taskRun,
            TaskState fromState,
            TaskState toState,
            String reason,
            String detailsJson
    ) {
        return new TaskStateTransitionEntity(taskRun, fromState, toState, reason, detailsJson);
    }

    @PrePersist
    void prePersist() {
        if (transitionedAt == null) {
            transitionedAt = Instant.now();
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

    public TaskState getFromState() {
        return fromState;
    }

    public TaskState getToState() {
        return toState;
    }

    public String getReason() {
        return reason;
    }

    public String getDetailsJson() {
        return detailsJson;
    }

    public Instant getTransitionedAt() {
        return transitionedAt;
    }
}
