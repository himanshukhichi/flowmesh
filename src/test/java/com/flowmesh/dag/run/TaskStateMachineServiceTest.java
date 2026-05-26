package com.flowmesh.dag.run;

import com.flowmesh.timeline.TaskStateTransitionRepository;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TaskStateMachineServiceTest {

    private final TaskStateTransitionRepository transitionRepository = mock(TaskStateTransitionRepository.class);
    private final TaskStateMachineService service = new TaskStateMachineService(transitionRepository);

    @Test
    void rejectsInvalidTaskStateTransitions() {
        TaskRunEntity taskRun = mock(TaskRunEntity.class);
        when(taskRun.getState()).thenReturn(TaskState.PENDING);
        when(taskRun.getTaskId()).thenReturn("load");

        assertThatThrownBy(() -> service.transition(taskRun, TaskState.SUCCESS, "bad_jump"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("PENDING -> SUCCESS");

        verify(taskRun, never()).transitionTo(any(TaskState.class));
        verify(transitionRepository, never()).save(any());
    }
}
