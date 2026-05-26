package com.flowmesh.dag.run;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "flowmesh.scheduler")
public record FlowMeshSchedulerProperties(
        boolean enabled,
        int batchSize,
        int heartbeatTimeoutSecs
) {
    public FlowMeshSchedulerProperties {
        if (batchSize <= 0) {
            batchSize = 50;
        }
        if (heartbeatTimeoutSecs <= 0) {
            heartbeatTimeoutSecs = 30;
        }
    }
}
