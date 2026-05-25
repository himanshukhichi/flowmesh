package com.flowmesh.dag.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public record TaskDefinition(
        @NotBlank String taskId,
        @NotBlank String type,
        List<@NotBlank String> dependsOn,
        Map<String, Object> config,
        @Min(1) Integer timeoutSecs,
        @Min(0) Integer retries
) {
    public static final int DEFAULT_TIMEOUT_SECS = 300;
    public static final int DEFAULT_RETRIES = 3;

    public TaskDefinition {
        dependsOn = dependsOn == null ? List.of() : Collections.unmodifiableList(new ArrayList<>(dependsOn));
        config = config == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(config));
        timeoutSecs = timeoutSecs == null ? DEFAULT_TIMEOUT_SECS : timeoutSecs;
        retries = retries == null ? DEFAULT_RETRIES : retries;
    }
}
