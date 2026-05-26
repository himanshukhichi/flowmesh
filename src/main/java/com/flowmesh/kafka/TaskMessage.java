package com.flowmesh.kafka;

import java.util.UUID;

public record TaskMessage(
        UUID dagRunId,
        UUID taskRunId,
        String taskId,
        String type,
        String configJson,
        int attempt,
        int timeoutSecs,
        int retries,
        String idempotencyKey,
        String traceId
) {
    public String topic() {
        return "tasks." + type;
    }
}
