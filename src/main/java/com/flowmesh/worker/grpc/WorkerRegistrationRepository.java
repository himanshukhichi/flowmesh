package com.flowmesh.worker.grpc;

import org.springframework.data.jpa.repository.JpaRepository;

public interface WorkerRegistrationRepository extends JpaRepository<WorkerRegistrationEntity, String> {
}
