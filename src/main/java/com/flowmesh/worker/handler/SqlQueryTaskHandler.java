package com.flowmesh.worker.handler;

import org.springframework.stereotype.Component;

@Component
public class SqlQueryTaskHandler implements TaskHandler {
    @Override
    public String type() {
        return "sql_query";
    }

    @Override
    public TaskHandlerResult execute(TaskContext context) {
        if (context.config().path("fail").asBoolean(false)) {
            return TaskHandlerResult.failure("sql_query configured to fail");
        }
        return TaskHandlerResult.success("sql_query completed");
    }
}
