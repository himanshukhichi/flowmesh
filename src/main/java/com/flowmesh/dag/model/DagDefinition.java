package com.flowmesh.dag.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public record DagDefinition(
        @NotBlank String dagId,
        @NotBlank String name,
        @NotEmpty @Valid List<TaskDefinition> tasks
) {
    public DagDefinition {
        tasks = tasks == null ? List.of() : Collections.unmodifiableList(new ArrayList<>(tasks));
    }
}
