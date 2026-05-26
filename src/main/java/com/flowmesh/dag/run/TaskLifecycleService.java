package com.flowmesh.dag.run;

import com.flowmesh.common.metrics.FlowMeshMetrics;
import com.flowmesh.dedup.TaskDeduplicationEntity;
import com.flowmesh.dedup.TaskDeduplicationRepository;
import com.flowmesh.kafka.DlqTaskMessage;
import com.flowmesh.kafka.RetryTaskMessage;
import com.flowmesh.kafka.TaskMessage;
import com.flowmesh.kafka.TaskPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

@Service
public class TaskLifecycleService {
    private final TaskRunRepository taskRunRepository;
    private final TaskDeduplicationRepository deduplicationRepository;
    private final TaskStateMachineService stateMachineService;
    private final TaskPublisher taskPublisher;
    private final FlowMeshMetrics metrics;

    public TaskLifecycleService(
            TaskRunRepository taskRunRepository,
            TaskDeduplicationRepository deduplicationRepository,
            TaskStateMachineService stateMachineService,
            TaskPublisher taskPublisher,
            FlowMeshMetrics metrics
    ) {
        this.taskRunRepository = taskRunRepository;
        this.deduplicationRepository = deduplicationRepository;
        this.stateMachineService = stateMachineService;
        this.taskPublisher = taskPublisher;
        this.metrics = metrics;
    }

    @Transactional(transactionManager = "transactionManager")
    public boolean startTask(TaskMessage message, String workerId) {
        TaskRunEntity taskRun = lockTask(message);
        if (taskRun.getState() != TaskState.QUEUED || taskRun.getAttempt() != message.attempt()) {
            return false;
        }

        if (deduplicationRepository.existsById(message.idempotencyKey())) {
            return false;
        }
        deduplicationRepository.save(TaskDeduplicationEntity.create(
                message.idempotencyKey(),
                message.dagRunId(),
                message.taskId(),
                workerId
        ));

        taskRun.assignWorker(workerId);
        stateMachineService.transition(taskRun, TaskState.RUNNING, "worker_started", details(workerId, null));
        return true;
    }

    @Transactional(transactionManager = "transactionManager")
    public void completeSuccess(TaskMessage message, String workerId, Instant startedAt) {
        TaskRunEntity taskRun = lockTask(message);
        if (!isCurrentExecution(taskRun, message, workerId)) {
            return;
        }

        stateMachineService.transition(taskRun, TaskState.SUCCESS, "worker_succeeded", details(workerId, null));
        metrics.recordExecutionLatency(taskRun.getType(), Duration.between(startedAt, Instant.now()).toMillis());
    }

    @Transactional(transactionManager = "transactionManager")
    public void completeFailure(TaskMessage message, String workerId, String errorMessage) {
        TaskRunEntity taskRun = lockTask(message);
        if (!isCurrentExecution(taskRun, message, workerId)) {
            return;
        }

        handleFailure(taskRun, message, workerId, errorMessage, "worker_failed");
    }

    @Transactional(transactionManager = "transactionManager")
    public void failRunningTask(TaskRunEntity taskRun, String errorMessage, String reason) {
        handleFailure(taskRun, toMessage(taskRun), taskRun.getWorkerId(), errorMessage, reason);
    }

    private void handleFailure(
            TaskRunEntity taskRun,
            TaskMessage message,
            String workerId,
            String errorMessage,
            String reason
    ) {
        taskRun.recordFailure(errorMessage);
        stateMachineService.transition(taskRun, TaskState.FAILED, reason, details(workerId, errorMessage));

        if (taskRun.hasRetriesRemaining()) {
            long delayMillis = retryDelayMillis(taskRun.getAttempt());
            taskRun.markRetrying(delayMillis, errorMessage);
            stateMachineService.transition(taskRun, TaskState.RETRYING, "retry_scheduled", details(workerId, errorMessage));
            deduplicationRepository.deleteById(message.idempotencyKey());
            metrics.recordRetry();
            taskPublisher.publishRetry(new RetryTaskMessage(
                    message.dagRunId(),
                    message.taskRunId(),
                    message.taskId(),
                    message.type(),
                    taskRun.getAttempt(),
                    taskRun.getNextAttemptAt().toEpochMilli(),
                    errorMessage,
                    message.idempotencyKey(),
                    message.traceId()
            ));
            return;
        }

        if (taskRun.getFailureBranchTaskId() != null) {
            return;
        }

        stateMachineService.transition(taskRun, TaskState.DLQ, "retry_exhausted", details(workerId, errorMessage));
        metrics.recordDlq();
        taskPublisher.publishDlq(new DlqTaskMessage(
                message.dagRunId(),
                message.taskRunId(),
                message.taskId(),
                message.type(),
                message.configJson(),
                errorMessage,
                message.idempotencyKey(),
                message.traceId()
        ));
    }

    @Transactional(transactionManager = "transactionManager")
    public void recordHeartbeat(String dagRunId, String taskId, String workerId) {
        taskRunRepository.lockByDagRunIdAndTaskId(UUID.fromString(dagRunId), taskId)
                .ifPresent(taskRun -> taskRun.recordHeartbeat(workerId));
    }

    private TaskRunEntity lockTask(TaskMessage message) {
        return taskRunRepository.lockByDagRunIdAndTaskId(message.dagRunId(), message.taskId())
                .orElseThrow(() -> new IllegalStateException(
                        "Task run not found for " + message.dagRunId() + ":" + message.taskId()
                ));
    }

    private long retryDelayMillis(int currentAttempt) {
        return (1L << Math.min(currentAttempt, 6)) * 1_000L;
    }

    private TaskMessage toMessage(TaskRunEntity taskRun) {
        String traceId = taskRun.getDagRun().getId() + ":" + taskRun.getTaskId() + ":" + taskRun.getAttempt();
        return new TaskMessage(
                taskRun.getDagRun().getId(),
                taskRun.getId(),
                taskRun.getTaskId(),
                taskRun.getType(),
                taskRun.getConfigJson(),
                taskRun.getAttempt(),
                taskRun.getTimeoutSecs(),
                taskRun.getRetries(),
                taskRun.idempotencyKey(),
                traceId
        );
    }

    private String details(String workerId, String errorMessage) {
        String escapedWorker = workerId == null ? "" : workerId.replace("\"", "\\\"");
        String escapedError = errorMessage == null ? "" : errorMessage.replace("\"", "\\\"");
        return "{\"workerId\":\"" + escapedWorker + "\",\"errorMessage\":\"" + escapedError + "\"}";
    }

    private boolean isCurrentExecution(TaskRunEntity taskRun, TaskMessage message, String workerId) {
        if (taskRun.getState() != TaskState.RUNNING || taskRun.getAttempt() != message.attempt()) {
            return false;
        }
        return taskRun.getWorkerId() == null || taskRun.getWorkerId().equals(workerId);
    }
}
