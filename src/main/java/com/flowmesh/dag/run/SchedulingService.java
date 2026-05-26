package com.flowmesh.dag.run;

import com.flowmesh.common.logging.MdcScopes;
import com.flowmesh.common.metrics.FlowMeshMetrics;
import com.flowmesh.kafka.TaskMessage;
import com.flowmesh.kafka.TaskPublisher;
import com.flowmesh.scheduler.pause.SchedulingPauseService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
public class SchedulingService {
    private final TaskRunRepository taskRunRepository;
    private final TaskStateMachineService stateMachineService;
    private final TaskPublisher taskPublisher;
    private final SchedulingPauseService pauseService;
    private final FlowMeshMetrics metrics;

    public SchedulingService(
            TaskRunRepository taskRunRepository,
            TaskStateMachineService stateMachineService,
            TaskPublisher taskPublisher,
            SchedulingPauseService pauseService,
            FlowMeshMetrics metrics
    ) {
        this.taskRunRepository = taskRunRepository;
        this.stateMachineService = stateMachineService;
        this.taskPublisher = taskPublisher;
        this.pauseService = pauseService;
        this.metrics = metrics;
    }

    @Transactional(transactionManager = "transactionManager")
    public List<TaskDispatch> queueReadyTasks(int limit) {
        promoteDueRetries(limit);

        List<TaskDispatch> dispatches = new ArrayList<>();
        for (TaskRunEntity taskRun : taskRunRepository.lockReadyPendingTasks(limit)) {
            if (pauseService.isPaused(taskRun.getDagRun().getDagId())) {
                continue;
            }

            try (MdcScopes.Scope ignored = MdcScopes.task(taskRun.getDagRun().getId(), taskRun.getTaskId(), null)) {
                stateMachineService.transition(taskRun, TaskState.QUEUED, "scheduler_ready");
                TaskDispatch dispatch = TaskDispatch.from(taskRun);
                taskPublisher.publish(toMessage(dispatch));
                metrics.recordTaskScheduled();
                recordSchedulingLatency(taskRun);
                dispatches.add(dispatch);
            }
        }
        return List.copyOf(dispatches);
    }

    private void promoteDueRetries(int limit) {
        for (TaskRunEntity taskRun : taskRunRepository.lockDueRetryingTasks(limit)) {
            taskRun.markPendingForRetry();
            stateMachineService.transition(taskRun, TaskState.PENDING, "retry_backoff_elapsed");
        }
    }

    private void recordSchedulingLatency(TaskRunEntity taskRun) {
        if (taskRun.getCreatedAt() != null) {
            metrics.recordSchedulingLatency(Duration.between(taskRun.getCreatedAt(), Instant.now()).toMillis());
        }
    }

    private TaskMessage toMessage(TaskDispatch dispatch) {
        UUID dagRunId = dispatch.dagRunId();
        String traceId = dagRunId + ":" + dispatch.taskId() + ":" + dispatch.attempt();
        return new TaskMessage(
                dagRunId,
                dispatch.taskRunId(),
                dispatch.taskId(),
                dispatch.type(),
                dispatch.configJson(),
                dispatch.attempt(),
                dispatch.timeoutSecs(),
                dispatch.retries(),
                dispatch.idempotencyKey(),
                traceId
        );
    }
}
