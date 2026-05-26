package com.flowmesh.worker.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.flowmesh.kafka.TaskMessage;

public record TaskContext(
        TaskMessage message,
        JsonNode config
) {
}
