package com.flowmesh.kafka.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

@Entity
@Table(name = "task_dispatch_outbox")
public class TaskDispatchOutboxEntity {
    @Id
    @GeneratedValue
    private UUID id;

    @Column(nullable = false, length = 240)
    private String topic;

    @Column(name = "message_key", nullable = false, length = 320)
    private String messageKey;

    @Column(name = "payload_type", nullable = false, length = 32)
    private String payloadType;

    @Column(name = "payload_json", nullable = false, columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private String payloadJson;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 24)
    private OutboxState state;

    @Column(name = "publish_attempts", nullable = false)
    private int publishAttempts;

    @Column(name = "next_attempt_at")
    private Instant nextAttemptAt;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Column(name = "error_message")
    private String errorMessage;

    protected TaskDispatchOutboxEntity() {
    }

    private TaskDispatchOutboxEntity(String topic, String messageKey, String payloadType, String payloadJson) {
        this.topic = topic;
        this.messageKey = messageKey;
        this.payloadType = payloadType;
        this.payloadJson = payloadJson;
        this.state = OutboxState.PENDING;
        this.publishAttempts = 0;
    }

    public static TaskDispatchOutboxEntity create(
            String topic,
            String messageKey,
            String payloadType,
            String payloadJson
    ) {
        return new TaskDispatchOutboxEntity(topic, messageKey, payloadType, payloadJson);
    }

    public void markSent() {
        this.state = OutboxState.SENT;
        this.publishedAt = Instant.now();
        this.errorMessage = null;
    }

    public void markPublishFailed(String errorMessage) {
        this.publishAttempts += 1;
        this.errorMessage = errorMessage;
        this.nextAttemptAt = Instant.now().plus(Math.min(publishAttempts, 30), ChronoUnit.SECONDS);
    }

    @PrePersist
    void prePersist() {
        Instant now = Instant.now();
        if (createdAt == null) {
            createdAt = now;
        }
        if (updatedAt == null) {
            updatedAt = now;
        }
    }

    @PreUpdate
    void preUpdate() {
        updatedAt = Instant.now();
    }

    public String getTopic() {
        return topic;
    }

    public String getMessageKey() {
        return messageKey;
    }

    public String getPayloadType() {
        return payloadType;
    }

    public String getPayloadJson() {
        return payloadJson;
    }
}
