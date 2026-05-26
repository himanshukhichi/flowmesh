package com.flowmesh.worker.handler;

import org.springframework.stereotype.Component;

@Component
public class HttpCallTaskHandler implements TaskHandler {
    @Override
    public String type() {
        return "http_call";
    }

    @Override
    public TaskHandlerResult execute(TaskContext context) {
        if (context.config().path("fail").asBoolean(false)) {
            return TaskHandlerResult.failure("http_call configured to fail");
        }
        return TaskHandlerResult.success("http_call completed");
    }
}
