package com.flowmesh.dlq;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface DlqTaskRepository extends JpaRepository<DlqTaskEntity, UUID> {
    List<DlqTaskEntity> findTop100ByOrderByCreatedAtDesc();
}
