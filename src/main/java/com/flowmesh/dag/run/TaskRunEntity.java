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
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
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

    @Column(name = "config_json", nullable = false, columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private String configJson;

    @Column(name = "success_branch_task_id", length = 160)
    private String successBranchTaskId;

    @Column(name = "failure_branch_task_id", length = 160)
    private String failureBranchTaskId;

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

    @Column(name = "next_attempt_at")
    private Instant nextAttemptAt;

    @Column(name = "last_heartbeat_at")
    private Instant lastHeartbeatAt;

    @Column(name = "worker_id", length = 160)
    private String workerId;

    @Column(name = "error_message")
    private String errorMessage;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected TaskRunEntity() {
    }

    private TaskRunEntity(DagRunEntity dagRun, TaskDefinition task, TaskState initialState, String configJson) {
        this.dagRun = dagRun;
        this.taskId = task.taskId();
        this.type = task.type();
        this.configJson = configJson;
        this.successBranchTaskId = task.successBranch();
        this.failureBranchTaskId = task.failureBranch();
        this.dependsOnTaskIds = new LinkedHashSet<>(task.dependsOn());
        this.state = initialState;
        this.attempt = 0;
        this.timeoutSecs = task.timeoutSecs();
        this.retries = task.retries();
    }

    public static TaskRunEntity create(DagRunEntity dagRun, TaskDefinition task, TaskState initialState, String configJson) {
        return new TaskRunEntity(dagRun, task, initialState, configJson);
    }

    public void transitionTo(TaskState nextState) {
        this.state = nextState;
        Instant now = Instant.now();
        if (nextState == TaskState.QUEUED) {
            queuedAt = now;
        } else if (nextState == TaskState.RUNNING) {
            startedAt = now;
            lastHeartbeatAt = now;
        } else if (nextState == TaskState.SUCCESS || nextState == TaskState.FAILED || nextState == TaskState.DLQ) {
            finishedAt = now;
            workerId = null;
        } else if (nextState == TaskState.RETRYING) {
            workerId = null;
        }
    }

    public void assignWorker(String workerId) {
        this.workerId = workerId;
    }

    public void recordHeartbeat(String workerId) {
        this.workerId = workerId;
        this.lastHeartbeatAt = Instant.now();
    }

    public void recordFailure(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public void markRetrying(long delayMillis, String errorMessage) {
        this.attempt += 1;
        this.errorMessage = errorMessage;
        this.nextAttemptAt = Instant.now().plus(delayMillis, ChronoUnit.MILLIS);
    }

    public void markPendingForRetry() {
        this.nextAttemptAt = null;
    }

    public boolean hasRetriesRemaining() {
        return attempt < retries;
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

    public String getConfigJson() {
        return configJson;
    }

    public String getSuccessBranchTaskId() {
        return successBranchTaskId;
    }

    public String getFailureBranchTaskId() {
        return failureBranchTaskId;
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

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public Instant getNextAttemptAt() {
        return nextAttemptAt;
    }

    public Instant getLastHeartbeatAt() {
        return lastHeartbeatAt;
    }

    public String getWorkerId() {
        return workerId;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}
