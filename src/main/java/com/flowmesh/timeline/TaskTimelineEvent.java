package com.flowmesh.timeline;

import com.flowmesh.dag.run.TaskState;

import java.time.Instant;

public record TaskTimelineEvent(
        String taskId,
        TaskState fromState,
        TaskState toState,
        String reason,
        String detailsJson,
        Instant transitionedAt
) {
    public static TaskTimelineEvent from(TaskStateTransitionEntity entity) {
        return new TaskTimelineEvent(
                entity.getTaskId(),
                entity.getFromState(),
                entity.getToState(),
                entity.getReason(),
                entity.getDetailsJson(),
                entity.getTransitionedAt()
        );
    }
}
