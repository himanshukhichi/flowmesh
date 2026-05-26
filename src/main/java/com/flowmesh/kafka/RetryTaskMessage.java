package com.flowmesh.kafka;

import java.util.UUID;

public record RetryTaskMessage(
        UUID dagRunId,
        UUID taskRunId,
        String taskId,
        String type,
        int nextAttempt,
        long notBeforeEpochMillis,
        String errorMessage,
        String idempotencyKey,
        String traceId
) {
}
