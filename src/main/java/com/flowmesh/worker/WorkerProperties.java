package com.flowmesh.worker;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;
import java.util.UUID;

@ConfigurationProperties(prefix = "flowmesh.worker")
public record WorkerProperties(
        boolean enabled,
        String workerId,
        List<String> supportedTaskTypes,
        int maxConcurrent,
        String schedulerGrpcHost,
        int schedulerGrpcPort
) {
    public WorkerProperties {
        if (workerId == null || workerId.isBlank()) {
            workerId = "worker-" + UUID.randomUUID();
        }
        if (supportedTaskTypes == null || supportedTaskTypes.isEmpty()) {
            supportedTaskTypes = List.of("http_call", "sql_query", "ml_inference");
        }
        if (maxConcurrent <= 0) {
            maxConcurrent = 4;
        }
        if (schedulerGrpcHost == null || schedulerGrpcHost.isBlank()) {
            schedulerGrpcHost = "localhost";
        }
        if (schedulerGrpcPort <= 0) {
            schedulerGrpcPort = 9091;
        }
    }
}
