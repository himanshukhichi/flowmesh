package com.flowmesh.dag.api;

import com.flowmesh.dag.run.DagRunStatus;

import java.util.List;
import java.util.UUID;

public record DagRunResponse(
        UUID dagRunId,
        String dagId,
        int version,
        DagRunStatus status,
        List<String> readyTaskIds
) {
}
