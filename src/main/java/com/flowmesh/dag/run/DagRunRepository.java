package com.flowmesh.dag.run;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface DagRunRepository extends JpaRepository<DagRunEntity, UUID> {
}
