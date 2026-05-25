package com.flowmesh.dag.api;

import java.time.Instant;
import java.util.List;

public record DagSubmissionResponse(
        String dagId,
        int version,
        String name,
        int taskCount,
        List<String> executionOrder,
        Instant createdAt
) {
}
