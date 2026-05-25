package com.flowmesh.dag.run;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "flowmesh.scheduler")
public record FlowMeshSchedulerProperties(
        boolean enabled,
        int batchSize
) {
    public FlowMeshSchedulerProperties {
        if (batchSize <= 0) {
            batchSize = 50;
        }
    }
}
