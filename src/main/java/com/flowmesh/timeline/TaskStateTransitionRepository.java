package com.flowmesh.timeline;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface TaskStateTransitionRepository extends JpaRepository<TaskStateTransitionEntity, UUID> {
    List<TaskStateTransitionEntity> findByDagRunIdOrderByTransitionedAtAsc(UUID dagRunId);
}
