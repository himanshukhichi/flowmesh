package com.flowmesh.dag.run;

import com.flowmesh.scheduler.leader.RedisRedlockLeaderElection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;

@Component
@ConditionalOnProperty(prefix = "flowmesh.scheduler", name = "enabled", havingValue = "true")
public class TaskTimeoutEnforcer {
    private static final Logger log = LoggerFactory.getLogger(TaskTimeoutEnforcer.class);

    private final TaskRunRepository taskRunRepository;
    private final TaskLifecycleService taskLifecycleService;
    private final RedisRedlockLeaderElection leaderElection;
    private final int batchSize;
    private final int heartbeatTimeoutSecs;

    public TaskTimeoutEnforcer(
            TaskRunRepository taskRunRepository,
            TaskLifecycleService taskLifecycleService,
            RedisRedlockLeaderElection leaderElection,
            FlowMeshSchedulerProperties properties
    ) {
        this.taskRunRepository = taskRunRepository;
        this.taskLifecycleService = taskLifecycleService;
        this.leaderElection = leaderElection;
        this.batchSize = properties.batchSize();
        this.heartbeatTimeoutSecs = properties.heartbeatTimeoutSecs();
    }

    @Scheduled(fixedDelayString = "${flowmesh.scheduler.timeout-scan-delay-ms:5000}")
    @Transactional(transactionManager = "transactionManager")
    public void enforceTimeouts() {
        if (!leaderElection.isLeader()) {
            return;
        }

        Set<UUID> handled = new LinkedHashSet<>();
        for (TaskRunEntity taskRun : taskRunRepository.lockTimedOutRunningTasks(batchSize)) {
            handled.add(taskRun.getId());
            log.warn("Task timed out dagRunId={} taskId={}", taskRun.getDagRun().getId(), taskRun.getTaskId());
            taskLifecycleService.failRunningTask(taskRun, "Task timed out after " + taskRun.getTimeoutSecs() + "s", "task_timeout");
        }

        for (TaskRunEntity taskRun : taskRunRepository.lockHeartbeatExpiredTasks(heartbeatTimeoutSecs, batchSize)) {
            if (handled.contains(taskRun.getId())) {
                continue;
            }
            log.warn("Task heartbeat expired dagRunId={} taskId={}", taskRun.getDagRun().getId(), taskRun.getTaskId());
            taskLifecycleService.failRunningTask(taskRun, "Worker heartbeat expired", "heartbeat_expired");
        }
    }
}
