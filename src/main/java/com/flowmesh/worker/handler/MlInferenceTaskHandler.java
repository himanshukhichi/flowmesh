package com.flowmesh.worker.handler;

import org.springframework.stereotype.Component;

@Component
public class MlInferenceTaskHandler implements TaskHandler {
    @Override
    public String type() {
        return "ml_inference";
    }

    @Override
    public TaskHandlerResult execute(TaskContext context) {
        if (context.config().path("fail").asBoolean(false)) {
            return TaskHandlerResult.failure("ml_inference configured to fail");
        }
        return TaskHandlerResult.success("ml_inference completed");
    }
}
