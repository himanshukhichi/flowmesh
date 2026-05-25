package com.flowmesh.dag.run;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.UUID;

public interface TaskRunRepository extends JpaRepository<TaskRunEntity, UUID> {
    List<TaskRunEntity> findByDagRun_IdOrderByCreatedAtAsc(UUID dagRunId);

    @Query(value = """
            select tr.*
            from task_runs tr
            where tr.state = 'PENDING'
              and not exists (
                  select 1
                  from task_run_dependencies dep
                  join task_runs dep_run
                    on dep_run.dag_run_id = tr.dag_run_id
                   and dep_run.task_id = dep.depends_on_task_id
                  where dep.task_run_id = tr.id
                    and dep_run.state <> 'SUCCESS'
              )
            order by tr.created_at
            limit :limit
            for update skip locked
            """, nativeQuery = true)
    List<TaskRunEntity> lockReadyPendingTasks(@Param("limit") int limit);
}
