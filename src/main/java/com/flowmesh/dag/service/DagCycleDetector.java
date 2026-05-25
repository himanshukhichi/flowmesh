package com.flowmesh.dag.service;

import com.flowmesh.dag.model.TaskDefinition;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Component
public class DagCycleDetector {
    private enum VisitState {
        VISITING,
        VISITED
    }

    public Optional<List<String>> findCycle(Map<String, TaskDefinition> tasksById) {
        Map<String, VisitState> stateByTaskId = new HashMap<>();
        List<String> stack = new ArrayList<>();
        Map<String, Integer> stackIndexByTaskId = new HashMap<>();

        for (String taskId : tasksById.keySet()) {
            if (!stateByTaskId.containsKey(taskId)) {
                Optional<List<String>> cycle = dfs(taskId, tasksById, stateByTaskId, stack, stackIndexByTaskId);
                if (cycle.isPresent()) {
                    return cycle;
                }
            }
        }

        return Optional.empty();
    }

    private Optional<List<String>> dfs(
            String taskId,
            Map<String, TaskDefinition> tasksById,
            Map<String, VisitState> stateByTaskId,
            List<String> stack,
            Map<String, Integer> stackIndexByTaskId
    ) {
        stateByTaskId.put(taskId, VisitState.VISITING);
        stackIndexByTaskId.put(taskId, stack.size());
        stack.add(taskId);

        for (String dependencyId : tasksById.get(taskId).dependsOn()) {
            VisitState dependencyState = stateByTaskId.get(dependencyId);
            if (dependencyState == VisitState.VISITING) {
                int cycleStart = stackIndexByTaskId.get(dependencyId);
                List<String> cycle = new ArrayList<>(stack.subList(cycleStart, stack.size()));
                cycle.add(dependencyId);
                return Optional.of(cycle);
            }

            if (dependencyState == null) {
                Optional<List<String>> cycle = dfs(
                        dependencyId,
                        tasksById,
                        stateByTaskId,
                        stack,
                        stackIndexByTaskId
                );
                if (cycle.isPresent()) {
                    return cycle;
                }
            }
        }

        stack.remove(stack.size() - 1);
        stackIndexByTaskId.remove(taskId);
        stateByTaskId.put(taskId, VisitState.VISITED);
        return Optional.empty();
    }
}
