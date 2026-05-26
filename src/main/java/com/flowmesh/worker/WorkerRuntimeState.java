package com.flowmesh.worker;

import com.flowmesh.kafka.TaskMessage;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
public class WorkerRuntimeState {
    private final ConcurrentMap<String, TaskMessage> currentTasks = new ConcurrentHashMap<>();

    public void started(TaskMessage message) {
        currentTasks.put(message.idempotencyKey(), message);
    }

    public void finished(TaskMessage message) {
        currentTasks.remove(message.idempotencyKey(), message);
    }

    public Optional<TaskMessage> currentTask() {
        return currentTasks.values().stream().findFirst();
    }

    public List<TaskMessage> currentTasks() {
        return List.copyOf(currentTasks.values());
    }
}
