package com.flowmesh.dedup;

import org.springframework.data.jpa.repository.JpaRepository;

public interface TaskDeduplicationRepository extends JpaRepository<TaskDeduplicationEntity, String> {
}
