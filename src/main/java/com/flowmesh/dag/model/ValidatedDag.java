package com.flowmesh.dag.model;

import java.util.List;
import java.util.Map;

public record ValidatedDag(
        DagDefinition definition,
        Map<String, TaskDefinition> tasksById,
        Map<String, List<String>> dependentsByTaskId,
        List<String> executionOrder,
        List<String> initialReadyTaskIds
) {
}
