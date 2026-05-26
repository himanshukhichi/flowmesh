package com.flowmesh.dag.service;

import com.flowmesh.dag.model.DagDefinition;
import com.flowmesh.dag.model.TaskDefinition;
import com.flowmesh.dag.model.ValidatedDag;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class DagValidationService {
    private final DagCycleDetector cycleDetector;
    private final TopologicalSorter topologicalSorter;

    public DagValidationService(DagCycleDetector cycleDetector, TopologicalSorter topologicalSorter) {
        this.cycleDetector = cycleDetector;
        this.topologicalSorter = topologicalSorter;
    }

    public ValidatedDag validate(DagDefinition definition) {
        if (definition == null) {
            throw new DagValidationException("INVALID_DAG", "DAG definition is required");
        }

        Map<String, TaskDefinition> tasksById = new LinkedHashMap<>();
        for (TaskDefinition task : definition.tasks()) {
            if (task.taskId() == null || task.taskId().isBlank()) {
                throw new DagValidationException("INVALID_TASK", "Task id is required");
            }
            if (task.type() == null || task.type().isBlank()) {
                throw new DagValidationException("INVALID_TASK", "Task type is required for task '" + task.taskId() + "'");
            }
            if (tasksById.putIfAbsent(task.taskId(), task) != null) {
                throw new DagValidationException(
                        "DUPLICATE_TASK_ID",
                        "Task id '" + task.taskId() + "' is declared more than once"
                );
            }
        }

        for (TaskDefinition task : tasksById.values()) {
            for (String dependencyId : task.dependsOn()) {
                if (dependencyId == null || dependencyId.isBlank()) {
                    throw new DagValidationException(
                            "INVALID_DEPENDENCY",
                            "Task '" + task.taskId() + "' has a blank dependency"
                    );
                }
                if (!tasksById.containsKey(dependencyId)) {
                    throw new DagValidationException(
                            "UNKNOWN_DEPENDENCY",
                            "Task '" + task.taskId() + "' depends on unknown task '" + dependencyId + "'"
                    );
                }
            }
            validateBranchReference(tasksById, task, task.successBranch(), "success_branch");
            validateBranchReference(tasksById, task, task.failureBranch(), "failure_branch");
        }

        cycleDetector.findCycle(tasksById).ifPresent(path -> {
            throw new CycleDetectedException(path);
        });

        List<String> executionOrder = topologicalSorter.sort(tasksById);
        Map<String, List<String>> dependentsByTaskId = buildDependents(tasksById);
        List<String> initialReadyTaskIds = tasksById.values().stream()
                .filter(task -> task.dependsOn().isEmpty())
                .map(TaskDefinition::taskId)
                .toList();

        return new ValidatedDag(
                definition,
                Collections.unmodifiableMap(new LinkedHashMap<>(tasksById)),
                dependentsByTaskId,
                executionOrder,
                initialReadyTaskIds
        );
    }

    private void validateBranchReference(
            Map<String, TaskDefinition> tasksById,
            TaskDefinition task,
            String branchTaskId,
            String branchName
    ) {
        if (branchTaskId == null) {
            return;
        }
        if (!tasksById.containsKey(branchTaskId)) {
            throw new DagValidationException(
                    "UNKNOWN_BRANCH",
                    "Task '" + task.taskId() + "' has unknown " + branchName + " task '" + branchTaskId + "'"
            );
        }
        if (task.taskId().equals(branchTaskId)) {
            throw new DagValidationException(
                    "INVALID_BRANCH",
                    "Task '" + task.taskId() + "' cannot branch to itself"
            );
        }
        if (!tasksById.get(branchTaskId).dependsOn().contains(task.taskId())) {
            throw new DagValidationException(
                    "INVALID_BRANCH",
                    "Task '" + task.taskId() + "' " + branchName + " target '" + branchTaskId
                            + "' must depend on '" + task.taskId() + "'"
            );
        }
    }

    private Map<String, List<String>> buildDependents(Map<String, TaskDefinition> tasksById) {
        Map<String, List<String>> dependentsByTaskId = new LinkedHashMap<>();
        for (String taskId : tasksById.keySet()) {
            dependentsByTaskId.put(taskId, new ArrayList<>());
        }

        for (TaskDefinition task : tasksById.values()) {
            for (String dependencyId : task.dependsOn()) {
                dependentsByTaskId.get(dependencyId).add(task.taskId());
            }
        }

        Map<String, List<String>> immutable = new LinkedHashMap<>();
        dependentsByTaskId.forEach((taskId, dependents) -> immutable.put(taskId, List.copyOf(dependents)));
        return Collections.unmodifiableMap(immutable);
    }
}
