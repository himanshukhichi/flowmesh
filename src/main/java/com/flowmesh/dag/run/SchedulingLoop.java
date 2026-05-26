package com.flowmesh.dag.run;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import com.flowmesh.scheduler.leader.RedisRedlockLeaderElection;

import java.util.List;

@Component
@ConditionalOnProperty(prefix = "flowmesh.scheduler", name = "enabled", havingValue = "true")
public class SchedulingLoop {
    private static final Logger log = LoggerFactory.getLogger(SchedulingLoop.class);

    private final SchedulingService schedulingService;
    private final RedisRedlockLeaderElection leaderElection;
    private final int batchSize;

    public SchedulingLoop(
            SchedulingService schedulingService,
            RedisRedlockLeaderElection leaderElection,
            FlowMeshSchedulerProperties properties
    ) {
        this.schedulingService = schedulingService;
        this.leaderElection = leaderElection;
        this.batchSize = properties.batchSize();
    }

    @Scheduled(fixedDelayString = "${flowmesh.scheduler.poll-delay-ms:1000}")
    public void pollReadyTasks() {
        if (!leaderElection.isLeader()) {
            return;
        }
        List<TaskDispatch> dispatches = schedulingService.queueReadyTasks(batchSize);
        for (TaskDispatch dispatch : dispatches) {
            log.info(
                    "Queued task for dispatch dagRunId={} taskId={} topic={} idempotencyKey={}",
                    dispatch.dagRunId(),
                    dispatch.taskId(),
                    dispatch.topic(),
                    dispatch.idempotencyKey()
            );
        }
    }
}
