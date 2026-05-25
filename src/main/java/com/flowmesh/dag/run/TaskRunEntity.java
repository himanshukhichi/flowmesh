package com.flowmesh.dag.run;

import com.flowmesh.dag.model.TaskDefinition;
import jakarta.persistence.CollectionTable;
import jakarta.persistence.Column;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;

@Entity
@Table(
        name = "task_runs",
        uniqueConstraints = @UniqueConstraint(
                name = "uk_task_runs_dag_run_task",
                columnNames = {"dag_run_id", "task_id"}
        )
)
public class TaskRunEntity {
    @Id
    @GeneratedValue
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "dag_run_id", nullable = false)
    private DagRunEntity dagRun;

    @Column(name = "task_id", nullable = false, length = 160)
    private String taskId;

    @Column(nullable = false, length = 120)
    private String type;

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "task_run_dependencies", joinColumns = @JoinColumn(name = "task_run_id"))
    @Column(name = "depends_on_task_id", nullable = false, length = 160)
    private Set<String> dependsOnTaskIds = new LinkedHashSet<>();

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 24)
    private TaskState state;

    @Column(nullable = false)
    private int attempt;

    @Column(name = "timeout_secs", nullable = false)
    private int timeoutSecs;

    @Column(nullable = false)
    private int retries;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "queued_at")
    private Instant queuedAt;

    @Column(name = "started_at")
    private Instant startedAt;

    @Column(name = "finished_at")
    private Instant finishedAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected TaskRunEntity() {
    }

    private TaskRunEntity(DagRunEntity dagRun, TaskDefinition task, TaskState initialState) {
        this.dagRun = dagRun;
        this.taskId = task.taskId();
        this.type = task.type();
        this.dependsOnTaskIds = new LinkedHashSet<>(task.dependsOn());
        this.state = initialState;
        this.attempt = 0;
        this.timeoutSecs = task.timeoutSecs();
        this.retries = task.retries();
    }

    public static TaskRunEntity create(DagRunEntity dagRun, TaskDefinition task, TaskState initialState) {
        return new TaskRunEntity(dagRun, task, initialState);
    }

    public void transitionTo(TaskState nextState) {
        this.state = nextState;
        Instant now = Instant.now();
        if (nextState == TaskState.QUEUED) {
            queuedAt = now;
        } else if (nextState == TaskState.RUNNING) {
            startedAt = now;
        } else if (nextState == TaskState.SUCCESS || nextState == TaskState.FAILED || nextState == TaskState.DLQ) {
            finishedAt = now;
        }
    }

    public String idempotencyKey() {
        return dagRun.getId() + ":" + taskId;
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

    public DagRunEntity getDagRun() {
        return dagRun;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getType() {
        return type;
    }

    public Set<String> getDependsOnTaskIds() {
        return Set.copyOf(dependsOnTaskIds);
    }

    public TaskState getState() {
        return state;
    }

    public int getAttempt() {
        return attempt;
    }

    public int getTimeoutSecs() {
        return timeoutSecs;
    }

    public int getRetries() {
        return retries;
    }
}
