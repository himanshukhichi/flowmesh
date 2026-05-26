package com.flowmesh.common.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class FlowMeshMetrics {
    private final MeterRegistry registry;
    private final Counter leaderElectionEvents;
    private final Counter retryEvents;
    private final Counter dlqEvents;
    private final AtomicLong tasksScheduledPerSec = new AtomicLong();
    private final AtomicLong retryRate = new AtomicLong();
    private final AtomicLong dlqRate = new AtomicLong();
    private final AtomicLong kafkaQueueDepth = new AtomicLong();
    private final AtomicLong scheduledWindow = new AtomicLong();
    private final AtomicLong retryWindow = new AtomicLong();
    private final AtomicLong dlqWindow = new AtomicLong();
    private final Map<String, DistributionSummary> executionLatencyByType = new ConcurrentHashMap<>();
    private final DistributionSummary schedulingLatency;

    public FlowMeshMetrics(MeterRegistry registry) {
        this.registry = registry;
        this.leaderElectionEvents = Counter.builder("leader_election_events_total").register(registry);
        this.retryEvents = Counter.builder("retry_events_total").register(registry);
        this.dlqEvents = Counter.builder("dlq_events_total").register(registry);
        Gauge.builder("tasks_scheduled_per_sec", tasksScheduledPerSec, AtomicLong::get).register(registry);
        Gauge.builder("retry_rate", retryRate, AtomicLong::get).register(registry);
        Gauge.builder("dlq_rate", dlqRate, AtomicLong::get).register(registry);
        Gauge.builder("kafka_queue_depth", kafkaQueueDepth, AtomicLong::get).register(registry);
        this.schedulingLatency = DistributionSummary.builder("task_scheduling_latency_ms")
                .baseUnit("milliseconds")
                .publishPercentiles(0.99)
                .publishPercentileHistogram()
                .register(registry);
    }

    public void recordLeaderElectionEvent() {
        leaderElectionEvents.increment();
    }

    public void setTasksScheduledPerSec(long value) {
        tasksScheduledPerSec.set(value);
    }

    public void recordRetry() {
        retryEvents.increment();
        retryWindow.incrementAndGet();
    }

    public void recordDlq() {
        dlqEvents.increment();
        dlqWindow.incrementAndGet();
    }

    public void setKafkaQueueDepth(long depth) {
        kafkaQueueDepth.set(depth);
    }

    public void recordTaskScheduled() {
        scheduledWindow.incrementAndGet();
    }

    public void recordSchedulingLatency(long millis) {
        schedulingLatency.record(millis);
    }

    public void recordExecutionLatency(String taskType, long millis) {
        executionLatencyByType.computeIfAbsent(taskType, type -> DistributionSummary.builder("task_execution_latency_ms")
                        .tag("type", type)
                        .baseUnit("milliseconds")
                        .publishPercentiles(0.99)
                        .publishPercentileHistogram()
                        .register(registry))
                .record(millis);
    }

    @Scheduled(fixedRate = 1000)
    public void publishRates() {
        tasksScheduledPerSec.set(scheduledWindow.getAndSet(0));
        retryRate.set(retryWindow.getAndSet(0));
        dlqRate.set(dlqWindow.getAndSet(0));
    }
}
