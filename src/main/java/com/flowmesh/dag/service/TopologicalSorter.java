package com.flowmesh.dag.service;

import com.flowmesh.dag.model.TaskDefinition;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

@Component
public class TopologicalSorter {

    public List<String> sort(Map<String, TaskDefinition> tasksById) {
        Map<String, Integer> inDegreeByTaskId = new LinkedHashMap<>();
        Map<String, List<String>> dependentsByTaskId = new LinkedHashMap<>();

        for (String taskId : tasksById.keySet()) {
            inDegreeByTaskId.put(taskId, 0);
            dependentsByTaskId.put(taskId, new ArrayList<>());
        }

        for (TaskDefinition task : tasksById.values()) {
            inDegreeByTaskId.put(task.taskId(), task.dependsOn().size());
            for (String dependencyId : task.dependsOn()) {
                dependentsByTaskId.get(dependencyId).add(task.taskId());
            }
        }

        Queue<String> ready = new ArrayDeque<>();
        inDegreeByTaskId.forEach((taskId, inDegree) -> {
            if (inDegree == 0) {
                ready.add(taskId);
            }
        });

        List<String> orderedTaskIds = new ArrayList<>(tasksById.size());
        while (!ready.isEmpty()) {
            String taskId = ready.remove();
            orderedTaskIds.add(taskId);

            for (String dependentId : dependentsByTaskId.get(taskId)) {
                int nextInDegree = inDegreeByTaskId.computeIfPresent(dependentId, (ignored, current) -> current - 1);
                if (nextInDegree == 0) {
                    ready.add(dependentId);
                }
            }
        }

        if (orderedTaskIds.size() != tasksById.size()) {
            throw new DagValidationException("INVALID_DAG", "Topological sort failed because the DAG is cyclic");
        }

        return List.copyOf(orderedTaskIds);
    }
}
