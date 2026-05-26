package com.flowmesh.kafka;

import java.util.UUID;

public record DlqTaskMessage(
        UUID dagRunId,
        UUID taskRunId,
        String taskId,
        String type,
        String payloadJson,
        String errorMessage,
        String idempotencyKey,
        String traceId
) {
}
