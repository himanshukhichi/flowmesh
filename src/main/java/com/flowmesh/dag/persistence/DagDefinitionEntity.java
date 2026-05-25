package com.flowmesh.dag.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(
        name = "dag_definitions",
        uniqueConstraints = @UniqueConstraint(
                name = "uk_dag_definitions_dag_id_version",
                columnNames = {"dag_id", "version"}
        )
)
public class DagDefinitionEntity {
    @Id
    @GeneratedValue
    private UUID id;

    @Column(name = "dag_id", nullable = false, length = 120)
    private String dagId;

    @Column(nullable = false)
    private int version;

    @Column(nullable = false, length = 240)
    private String name;

    @Column(name = "definition_json", nullable = false, columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private String definitionJson;

    @Column(name = "execution_order_json", nullable = false, columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private String executionOrderJson;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    protected DagDefinitionEntity() {
    }

    private DagDefinitionEntity(
            String dagId,
            int version,
            String name,
            String definitionJson,
            String executionOrderJson
    ) {
        this.dagId = dagId;
        this.version = version;
        this.name = name;
        this.definitionJson = definitionJson;
        this.executionOrderJson = executionOrderJson;
    }

    public static DagDefinitionEntity create(
            String dagId,
            int version,
            String name,
            String definitionJson,
            String executionOrderJson
    ) {
        return new DagDefinitionEntity(dagId, version, name, definitionJson, executionOrderJson);
    }

    @PrePersist
    void prePersist() {
        if (createdAt == null) {
            createdAt = Instant.now();
        }
    }

    public UUID getId() {
        return id;
    }

    public String getDagId() {
        return dagId;
    }

    public int getVersion() {
        return version;
    }

    public String getName() {
        return name;
    }

    public String getDefinitionJson() {
        return definitionJson;
    }

    public String getExecutionOrderJson() {
        return executionOrderJson;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
