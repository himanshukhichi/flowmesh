package com.flowmesh.dag.run;

import com.flowmesh.common.metrics.FlowMeshMetrics;
import com.flowmesh.dedup.TaskDeduplicationEntity;
import com.flowmesh.dedup.TaskDeduplicationRepository;
import com.flowmesh.kafka.TaskMessage;
import com.flowmesh.kafka.TaskPublisher;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TaskLifecycleServiceTest {
    private final TaskRunRepository taskRunRepository = mock(TaskRunRepository.class);
    private final TaskDeduplicationRepository deduplicationRepository = mock(TaskDeduplicationRepository.class);
    private final TaskStateMachineService stateMachineService = mock(TaskStateMachineService.class);
    private final TaskPublisher taskPublisher = mock(TaskPublisher.class);
    private final FlowMeshMetrics metrics = mock(FlowMeshMetrics.class);
    private final TaskLifecycleService service = new TaskLifecycleService(
            taskRunRepository,
            deduplicationRepository,
            stateMachineService,
            taskPublisher,
            metrics
    );

    @Test
    void reservesIdempotencyKeyBeforeStartingTask() {
        TaskMessage message = message(0);
        TaskRunEntity taskRun = taskRun(TaskState.QUEUED, 0, null);
        when(taskRunRepository.lockByDagRunIdAndTaskId(message.dagRunId(), message.taskId()))
                .thenReturn(Optional.of(taskRun));
        when(deduplicationRepository.existsById(message.idempotencyKey())).thenReturn(false);

        boolean started = service.startTask(message, "worker-1");

        assertThat(started).isTrue();
        verify(deduplicationRepository).save(any(TaskDeduplicationEntity.class));
        verify(taskRun).assignWorker("worker-1");
        verify(stateMachineService).transition(taskRun, TaskState.RUNNING, "worker_started", "{\"workerId\":\"worker-1\",\"errorMessage\":\"\"}");
    }

    @Test
    void rejectsMessagesForStaleAttempts() {
        TaskMessage message = message(0);
        TaskRunEntity taskRun = taskRun(TaskState.QUEUED, 1, null);
        when(taskRunRepository.lockByDagRunIdAndTaskId(message.dagRunId(), message.taskId()))
                .thenReturn(Optional.of(taskRun));

        boolean started = service.startTask(message, "worker-1");

        assertThat(started).isFalse();
        verify(deduplicationRepository, never()).save(any());
        verify(stateMachineService, never()).transition(any(), any(), any(), any());
    }

    @Test
    void ignoresStaleCompletionMessages() {
        TaskMessage message = message(0);
        TaskRunEntity taskRun = taskRun(TaskState.RUNNING, 1, "worker-1");
        when(taskRunRepository.lockByDagRunIdAndTaskId(message.dagRunId(), message.taskId()))
                .thenReturn(Optional.of(taskRun));

        service.completeSuccess(message, "worker-1", Instant.now());

        verify(stateMachineService, never()).transition(any(), any(), any(), any());
        verify(metrics, never()).recordExecutionLatency(any(), anyLong());
    }

    private TaskMessage message(int attempt) {
        UUID dagRunId = UUID.randomUUID();
        UUID taskRunId = UUID.randomUUID();
        return new TaskMessage(
                dagRunId,
                taskRunId,
                "load",
                "sql_query",
                "{}",
                attempt,
                60,
                3,
                dagRunId + ":load",
                dagRunId + ":load:" + attempt
        );
    }

    private TaskRunEntity taskRun(TaskState state, int attempt, String workerId) {
        TaskRunEntity taskRun = mock(TaskRunEntity.class);
        when(taskRun.getState()).thenReturn(state);
        when(taskRun.getAttempt()).thenReturn(attempt);
        when(taskRun.getWorkerId()).thenReturn(workerId);
        return taskRun;
    }
}
