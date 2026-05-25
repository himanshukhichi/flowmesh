package com.flowmesh.dag.api;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record DagVersionResponse(
        UUID id,
        String dagId,
        int version,
        String name,
        int taskCount,
        List<String> executionOrder,
        Instant createdAt
) {
}
