package com.flowmesh.worker.handler;

public interface TaskHandler {
    String type();

    TaskHandlerResult execute(TaskContext context) throws Exception;
}
