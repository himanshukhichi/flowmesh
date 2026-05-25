package com.flowmesh.dag.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface DagDefinitionRepository extends JpaRepository<DagDefinitionEntity, UUID> {
    Optional<DagDefinitionEntity> findByDagIdAndVersion(String dagId, int version);

    Optional<DagDefinitionEntity> findTopByDagIdOrderByVersionDesc(String dagId);
}
