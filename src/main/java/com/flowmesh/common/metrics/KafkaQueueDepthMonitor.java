package com.flowmesh.common.metrics;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

@Component
@ConditionalOnProperty(prefix = "flowmesh.observability", name = "kafka-depth-enabled", havingValue = "true")
public class KafkaQueueDepthMonitor {
    private final FlowMeshMetrics metrics;
    private final String bootstrapServers;
    private final String workerGroupId;
    private final String retryGroupId;
    private final String dlqGroupId;

    public KafkaQueueDepthMonitor(
            FlowMeshMetrics metrics,
            @Value("${spring.kafka.bootstrap-servers:localhost:29092}") String bootstrapServers,
            @Value("${flowmesh.worker.group-id:flowmesh-workers}") String workerGroupId,
            @Value("${flowmesh.retry.group-id:flowmesh-retry}") String retryGroupId,
            @Value("${flowmesh.dlq.group-id:flowmesh-dlq}") String dlqGroupId
    ) {
        this.metrics = metrics;
        this.bootstrapServers = bootstrapServers;
        this.workerGroupId = workerGroupId;
        this.retryGroupId = retryGroupId;
        this.dlqGroupId = dlqGroupId;
    }

    @Scheduled(fixedDelayString = "${flowmesh.observability.kafka-depth-scan-ms:10000}")
    public void measureDepth() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        try (AdminClient adminClient = AdminClient.create(properties)) {
            Set<String> taskTopics = adminClient.listTopics().names().get().stream()
                    .filter(topic -> topic.startsWith("tasks."))
                    .collect(Collectors.toSet());
            Map<TopicPartition, OffsetSpec> latestOffsets = new HashMap<>();
            for (String topic : taskTopics) {
                adminClient.describeTopics(Set.of(topic))
                        .allTopicNames()
                        .get()
                        .get(topic)
                        .partitions()
                        .forEach(partition -> latestOffsets.put(new TopicPartition(topic, partition.partition()), OffsetSpec.latest()));
            }
            if (latestOffsets.isEmpty()) {
                metrics.setKafkaQueueDepth(0);
                return;
            }
            ListOffsetsResult result = adminClient.listOffsets(latestOffsets);
            Map<TopicPartition, Long> latestByPartition = result.all().get().entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().offset()));
            long depth = queueDepth(adminClient, latestByPartition, taskTopics);
            metrics.setKafkaQueueDepth(depth);
        } catch (Exception ignored) {
            metrics.setKafkaQueueDepth(-1);
        }
    }

    private long queueDepth(
            AdminClient adminClient,
            Map<TopicPartition, Long> latestByPartition,
            Set<String> taskTopics
    ) throws Exception {
        Map<String, Map<TopicPartition, OffsetAndMetadata>> committedByGroup = new HashMap<>();
        for (String groupId : Set.of(workerGroupId, retryGroupId, dlqGroupId)) {
            committedByGroup.put(groupId, committedOffsets(adminClient, groupId));
        }

        long depth = 0;
        for (Map.Entry<TopicPartition, Long> entry : latestByPartition.entrySet()) {
            TopicPartition partition = entry.getKey();
            if (!taskTopics.contains(partition.topic())) {
                continue;
            }
            String groupId = groupForTopic(partition.topic());
            OffsetAndMetadata committed = committedByGroup.getOrDefault(groupId, Map.of()).get(partition);
            long committedOffset = committed == null ? 0 : committed.offset();
            depth += Math.max(0, entry.getValue() - committedOffset);
        }
        return depth;
    }

    private Map<TopicPartition, OffsetAndMetadata> committedOffsets(AdminClient adminClient, String groupId) {
        try {
            return adminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
        } catch (Exception exception) {
            return Map.of();
        }
    }

    private String groupForTopic(String topic) {
        if ("tasks.retry".equals(topic)) {
            return retryGroupId;
        }
        if ("tasks.dlq".equals(topic)) {
            return dlqGroupId;
        }
        return workerGroupId;
    }
}
