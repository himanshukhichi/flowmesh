package com.flowmesh.dag.run;

import java.util.UUID;

public record TaskDispatch(
        UUID dagRunId,
        String taskId,
        String type,
        String topic,
        String idempotencyKey
) {
    public static TaskDispatch from(TaskRunEntity taskRun) {
        return new TaskDispatch(
                taskRun.getDagRun().getId(),
                taskRun.getTaskId(),
                taskRun.getType(),
                "tasks." + taskRun.getType(),
                taskRun.idempotencyKey()
        );
    }
}
