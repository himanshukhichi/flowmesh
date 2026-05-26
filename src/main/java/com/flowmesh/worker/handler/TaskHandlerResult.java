package com.flowmesh.worker.handler;

public record TaskHandlerResult(
        boolean success,
        String message
) {
    public static TaskHandlerResult success(String message) {
        return new TaskHandlerResult(true, message);
    }

    public static TaskHandlerResult failure(String message) {
        return new TaskHandlerResult(false, message);
    }
}
