package com.flowmesh.dag.run;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface TaskRunRepository extends JpaRepository<TaskRunEntity, UUID> {
    List<TaskRunEntity> findByDagRun_IdOrderByCreatedAtAsc(UUID dagRunId);

    @Query(value = """
            select tr.*
            from task_runs tr
            where tr.state = 'PENDING'
              and (tr.next_attempt_at is null or tr.next_attempt_at <= now())
              and not exists (
                  select 1
                  from task_run_dependencies dep
                  join task_runs dep_run
                    on dep_run.dag_run_id = tr.dag_run_id
                   and dep_run.task_id = dep.depends_on_task_id
                  where dep.task_run_id = tr.id
                    and not (
                        (dep_run.state = 'SUCCESS'
                            and (dep_run.success_branch_task_id is null or dep_run.success_branch_task_id = tr.task_id))
                        or (dep_run.state = 'FAILED' and dep_run.failure_branch_task_id = tr.task_id)
                    )
              )
            order by tr.created_at
            limit :limit
            for update skip locked
            """, nativeQuery = true)
    List<TaskRunEntity> lockReadyPendingTasks(@Param("limit") int limit);

    @Query(value = """
            select tr.*
            from task_runs tr
            where tr.state = 'RETRYING'
              and tr.next_attempt_at <= now()
            order by tr.next_attempt_at
            limit :limit
            for update skip locked
            """, nativeQuery = true)
    List<TaskRunEntity> lockDueRetryingTasks(@Param("limit") int limit);

    @Query(value = """
            select tr.*
            from task_runs tr
            where tr.state = 'RUNNING'
              and tr.started_at is not null
              and tr.started_at + (tr.timeout_secs * interval '1 second') < now()
            order by tr.started_at
            limit :limit
            for update skip locked
            """, nativeQuery = true)
    List<TaskRunEntity> lockTimedOutRunningTasks(@Param("limit") int limit);

    @Query(value = """
            select tr.*
            from task_runs tr
            where tr.state = 'RUNNING'
              and tr.last_heartbeat_at is not null
              and tr.last_heartbeat_at < now() - (:heartbeatTimeoutSecs * interval '1 second')
            order by tr.last_heartbeat_at
            limit :limit
            for update skip locked
            """, nativeQuery = true)
    List<TaskRunEntity> lockHeartbeatExpiredTasks(
            @Param("heartbeatTimeoutSecs") int heartbeatTimeoutSecs,
            @Param("limit") int limit
    );

    @Query(value = """
            select tr.*
            from task_runs tr
            where tr.dag_run_id = :dagRunId
              and tr.task_id = :taskId
            for update
            """, nativeQuery = true)
    Optional<TaskRunEntity> lockByDagRunIdAndTaskId(@Param("dagRunId") UUID dagRunId, @Param("taskId") String taskId);

    Optional<TaskRunEntity> findByDagRun_IdAndTaskId(UUID dagRunId, String taskId);
}
