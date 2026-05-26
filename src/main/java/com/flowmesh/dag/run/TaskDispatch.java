package com.flowmesh.dag.run;

import java.util.UUID;

public record TaskDispatch(
        UUID dagRunId,
        UUID taskRunId,
        String taskId,
        String type,
        String topic,
        String idempotencyKey,
        int attempt,
        int timeoutSecs,
        int retries,
        String configJson
) {
    public static TaskDispatch from(TaskRunEntity taskRun) {
        return new TaskDispatch(
                taskRun.getDagRun().getId(),
                taskRun.getId(),
                taskRun.getTaskId(),
                taskRun.getType(),
                "tasks." + taskRun.getType(),
                taskRun.idempotencyKey(),
                taskRun.getAttempt(),
                taskRun.getTimeoutSecs(),
                taskRun.getRetries(),
                taskRun.getConfigJson()
        );
    }
}
