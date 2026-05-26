package com.flowmesh.dag.run;

import com.flowmesh.timeline.TaskStateTransitionEntity;
import com.flowmesh.timeline.TaskStateTransitionRepository;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Set;

@Service
public class TaskStateMachineService {
    private static final Map<TaskState, Set<TaskState>> ALLOWED_TRANSITIONS = Map.of(
            TaskState.CREATED, Set.of(TaskState.PENDING),
            TaskState.PENDING, Set.of(TaskState.QUEUED),
            TaskState.QUEUED, Set.of(TaskState.RUNNING),
            TaskState.RUNNING, Set.of(TaskState.SUCCESS, TaskState.FAILED),
            TaskState.FAILED, Set.of(TaskState.RETRYING, TaskState.DLQ),
            TaskState.RETRYING, Set.of(TaskState.PENDING),
            TaskState.DLQ, Set.of(TaskState.PENDING),
            TaskState.SUCCESS, Set.of()
    );

    private final TaskStateTransitionRepository transitionRepository;

    public TaskStateMachineService(TaskStateTransitionRepository transitionRepository) {
        this.transitionRepository = transitionRepository;
    }

    public void transition(TaskRunEntity taskRun, TaskState nextState, String reason) {
        transition(taskRun, nextState, reason, "{}");
    }

    public void transition(TaskRunEntity taskRun, TaskState nextState, String reason, String detailsJson) {
        TaskState previousState = taskRun.getState();
        if (!ALLOWED_TRANSITIONS.getOrDefault(previousState, Set.of()).contains(nextState)) {
            throw new IllegalStateException(
                    "Invalid task state transition " + previousState + " -> " + nextState
                            + " for task '" + taskRun.getTaskId() + "'"
            );
        }

        taskRun.transitionTo(nextState);
        transitionRepository.save(TaskStateTransitionEntity.create(
                taskRun,
                previousState,
                nextState,
                reason,
                detailsJson
        ));
    }
}
