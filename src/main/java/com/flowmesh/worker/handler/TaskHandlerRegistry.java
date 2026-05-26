package com.flowmesh.worker.handler;

import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class TaskHandlerRegistry {
    private final Map<String, TaskHandler> handlersByType;

    public TaskHandlerRegistry(List<TaskHandler> handlers) {
        this.handlersByType = handlers.stream().collect(Collectors.toUnmodifiableMap(TaskHandler::type, Function.identity()));
    }

    public TaskHandler handlerFor(String type) {
        TaskHandler handler = handlersByType.get(type);
        if (handler == null) {
            throw new IllegalArgumentException("No task handler registered for type '" + type + "'");
        }
        return handler;
    }
}
