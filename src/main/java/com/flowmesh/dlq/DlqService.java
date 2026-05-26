package com.flowmesh.dlq;

import com.flowmesh.dag.run.TaskRunEntity;
import com.flowmesh.dag.run.TaskRunRepository;
import com.flowmesh.dag.run.TaskState;
import com.flowmesh.dag.run.TaskStateMachineService;
import com.flowmesh.dedup.TaskDeduplicationRepository;
import com.flowmesh.kafka.DlqTaskMessage;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
public class DlqService {
    private final DlqTaskRepository dlqTaskRepository;
    private final TaskRunRepository taskRunRepository;
    private final TaskStateMachineService stateMachineService;
    private final TaskDeduplicationRepository deduplicationRepository;

    public DlqService(
            DlqTaskRepository dlqTaskRepository,
            TaskRunRepository taskRunRepository,
            TaskStateMachineService stateMachineService,
            TaskDeduplicationRepository deduplicationRepository
    ) {
        this.dlqTaskRepository = dlqTaskRepository;
        this.taskRunRepository = taskRunRepository;
        this.stateMachineService = stateMachineService;
        this.deduplicationRepository = deduplicationRepository;
    }

    @Transactional(transactionManager = "transactionManager")
    public void record(DlqTaskMessage message) {
        dlqTaskRepository.save(DlqTaskEntity.create(
                message.dagRunId(),
                message.taskId(),
                message.type(),
                message.idempotencyKey(),
                message.payloadJson(),
                message.errorMessage()
        ));
    }

    @Transactional(transactionManager = "transactionManager", readOnly = true)
    public List<DlqTaskResponse> inspect() {
        return dlqTaskRepository.findTop100ByOrderByCreatedAtDesc().stream()
                .map(DlqTaskResponse::from)
                .toList();
    }

    @Transactional(transactionManager = "transactionManager")
    public DlqTaskResponse requeue(UUID dlqTaskId) {
        DlqTaskEntity dlqTask = dlqTaskRepository.findById(dlqTaskId)
                .orElseThrow(() -> new IllegalArgumentException("DLQ task not found: " + dlqTaskId));
        TaskRunEntity taskRun = taskRunRepository.lockByDagRunIdAndTaskId(dlqTask.getDagRunId(), dlqTask.getTaskId())
                .orElseThrow(() -> new IllegalArgumentException("Task run not found for DLQ task " + dlqTaskId));
        deduplicationRepository.deleteById(dlqTask.getIdempotencyKey());
        taskRun.markPendingForRetry();
        stateMachineService.transition(taskRun, TaskState.PENDING, "dlq_manual_requeue");
        dlqTask.markRequeued();
        return DlqTaskResponse.from(dlqTask);
    }
}
