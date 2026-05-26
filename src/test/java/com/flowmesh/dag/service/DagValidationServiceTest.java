package com.flowmesh.dag.service;

import com.flowmesh.dag.model.DagDefinition;
import com.flowmesh.dag.model.TaskDefinition;
import com.flowmesh.dag.model.ValidatedDag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DagValidationServiceTest {
    private final DagValidationService service = new DagValidationService(
            new DagCycleDetector(),
            new TopologicalSorter()
    );

    @Test
    void topologicallySortsDagUsingKahnsAlgorithm() {
        DagDefinition definition = new DagDefinition(
                "daily-pipeline",
                "Daily Pipeline",
                List.of(
                        task("extract"),
                        task("audit"),
                        task("transform", "extract"),
                        task("load", "transform", "audit")
                )
        );

        ValidatedDag validatedDag = service.validate(definition);

        assertThat(validatedDag.initialReadyTaskIds()).containsExactly("extract", "audit");
        assertThat(validatedDag.executionOrder()).containsExactly("extract", "audit", "transform", "load");
        assertThat(validatedDag.dependentsByTaskId().get("extract")).containsExactly("transform");
    }

    @Test
    void detectsCycleAndReturnsCyclePath() {
        DagDefinition definition = new DagDefinition(
                "cyclic",
                "Cyclic",
                List.of(
                        task("taskA", "taskB"),
                        task("taskB", "taskA")
                )
        );

        assertThatThrownBy(() -> service.validate(definition))
                .isInstanceOfSatisfying(CycleDetectedException.class, exception -> {
                    assertThat(exception.error()).isEqualTo("CYCLE_DETECTED");
                    assertThat(exception.path()).containsExactly("taskA", "taskB", "taskA");
                });
    }

    @Test
    void rejectsUnknownDependencies() {
        DagDefinition definition = new DagDefinition(
                "missing-dependency",
                "Missing Dependency",
                List.of(task("load", "extract"))
        );

        assertThatThrownBy(() -> service.validate(definition))
                .isInstanceOfSatisfying(DagValidationException.class, exception -> {
                    assertThat(exception.error()).isEqualTo("UNKNOWN_DEPENDENCY");
                    assertThat(exception.getMessage()).contains("extract");
                });
    }

    @Test
    void rejectsDuplicateTaskIds() {
        DagDefinition definition = new DagDefinition(
                "duplicates",
                "Duplicates",
                List.of(task("extract"), task("extract"))
        );

        assertThatThrownBy(() -> service.validate(definition))
                .isInstanceOfSatisfying(DagValidationException.class, exception ->
                        assertThat(exception.error()).isEqualTo("DUPLICATE_TASK_ID"));
    }

    @Test
    void rejectsUnknownBranchTargets() {
        DagDefinition definition = new DagDefinition(
                "bad-branch",
                "Bad Branch",
                List.of(
                        taskWithBranches("classify", "ship", "fallback"),
                        task("ship", "classify")
                )
        );

        assertThatThrownBy(() -> service.validate(definition))
                .isInstanceOfSatisfying(DagValidationException.class, exception -> {
                    assertThat(exception.error()).isEqualTo("UNKNOWN_BRANCH");
                    assertThat(exception.getMessage()).contains("fallback");
                });
    }

    @Test
    void rejectsSelfBranchTargets() {
        DagDefinition definition = new DagDefinition(
                "self-branch",
                "Self Branch",
                List.of(taskWithBranches("classify", "classify", null))
        );

        assertThatThrownBy(() -> service.validate(definition))
                .isInstanceOfSatisfying(DagValidationException.class, exception ->
                        assertThat(exception.error()).isEqualTo("INVALID_BRANCH"));
    }

    @Test
    void rejectsBranchTargetsThatAreNotDownstreamTasks() {
        DagDefinition definition = new DagDefinition(
                "detached-branch",
                "Detached Branch",
                List.of(
                        taskWithBranches("classify", "ship", null),
                        task("ship")
                )
        );

        assertThatThrownBy(() -> service.validate(definition))
                .isInstanceOfSatisfying(DagValidationException.class, exception -> {
                    assertThat(exception.error()).isEqualTo("INVALID_BRANCH");
                    assertThat(exception.getMessage()).contains("must depend on 'classify'");
                });
    }

    @Test
    void acceptsBranchTargetsThatDependOnTheBranchingTask() {
        DagDefinition definition = new DagDefinition(
                "valid-branch",
                "Valid Branch",
                List.of(
                        taskWithBranches("classify", "ship", "fallback"),
                        task("ship", "classify"),
                        task("fallback", "classify")
                )
        );

        ValidatedDag validatedDag = service.validate(definition);

        assertThat(validatedDag.executionOrder()).containsExactly("classify", "ship", "fallback");
    }

    private TaskDefinition task(String taskId, String... dependsOn) {
        return new TaskDefinition(
                taskId,
                "http_call",
                List.of(dependsOn),
                Map.of("url", "https://example.test/" + taskId),
                60,
                3,
                null,
                null
        );
    }

    private TaskDefinition taskWithBranches(String taskId, String successBranch, String failureBranch) {
        return new TaskDefinition(
                taskId,
                "http_call",
                List.of(),
                Map.of("url", "https://example.test/" + taskId),
                60,
                3,
                successBranch,
                failureBranch
        );
    }
}
