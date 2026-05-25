package com.flowmesh.dag.run;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class SchedulingService {
    private final TaskRunRepository taskRunRepository;

    public SchedulingService(TaskRunRepository taskRunRepository) {
        this.taskRunRepository = taskRunRepository;
    }

    @Transactional
    public List<TaskDispatch> queueReadyTasks(int limit) {
        return taskRunRepository.lockReadyPendingTasks(limit).stream()
                .peek(taskRun -> taskRun.transitionTo(TaskState.QUEUED))
                .map(TaskDispatch::from)
                .toList();
    }
}
