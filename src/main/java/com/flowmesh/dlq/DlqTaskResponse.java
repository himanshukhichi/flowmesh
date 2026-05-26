package com.flowmesh.dlq;

import java.time.Instant;
import java.util.UUID;

public record DlqTaskResponse(
        UUID id,
        UUID dagRunId,
        String taskId,
        String taskType,
        String idempotencyKey,
        String payloadJson,
        String errorMessage,
        Instant createdAt,
        Instant requeuedAt
) {
    public static DlqTaskResponse from(DlqTaskEntity entity) {
        return new DlqTaskResponse(
                entity.getId(),
                entity.getDagRunId(),
                entity.getTaskId(),
                entity.getTaskType(),
                entity.getIdempotencyKey(),
                entity.getPayloadJson(),
                entity.getErrorMessage(),
                entity.getCreatedAt(),
                entity.getRequeuedAt()
        );
    }
}
