package com.flowmesh.scheduler.leader;

import com.flowmesh.common.metrics.FlowMeshMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@ConditionalOnProperty(prefix = "flowmesh.scheduler", name = "enabled", havingValue = "true")
public class RedisRedlockLeaderElection {
    private static final Logger log = LoggerFactory.getLogger(RedisRedlockLeaderElection.class);
    private static final String LOCK_KEY = "flowmesh:scheduler:leader";
    private static final DefaultRedisScript<Long> RENEW_SCRIPT = new DefaultRedisScript<>(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('pexpire', KEYS[1], ARGV[2]) else return 0 end",
            Long.class
    );

    private final StringRedisTemplate redisTemplate;
    private final FlowMeshMetrics metrics;
    private final AtomicBoolean leader = new AtomicBoolean(false);
    private final String token = UUID.randomUUID().toString();
    private final Duration ttl = Duration.ofSeconds(10);

    public RedisRedlockLeaderElection(StringRedisTemplate redisTemplate, FlowMeshMetrics metrics) {
        this.redisTemplate = redisTemplate;
        this.metrics = metrics;
    }

    public boolean isLeader() {
        return leader.get();
    }

    @Scheduled(fixedDelayString = "${flowmesh.scheduler.leader-renew-ms:5000}")
    public void renewOrAcquire() {
        if (leader.get() && renew()) {
            return;
        }

        Boolean acquired = redisTemplate.opsForValue().setIfAbsent(LOCK_KEY, token, ttl);
        boolean nowLeader = Boolean.TRUE.equals(acquired);
        boolean changed = leader.getAndSet(nowLeader) != nowLeader;
        if (changed || nowLeader) {
            metrics.recordLeaderElectionEvent();
            log.info("Scheduler leader status changed leader={}", nowLeader);
        }
    }

    private boolean renew() {
        Long renewed = redisTemplate.execute(
                RENEW_SCRIPT,
                List.of(LOCK_KEY),
                token,
                String.valueOf(ttl.toMillis())
        );
        boolean renewedLock = Long.valueOf(1L).equals(renewed);
        if (!renewedLock) {
            leader.set(false);
            metrics.recordLeaderElectionEvent();
            log.warn("Scheduler leadership renewal failed");
        }
        return renewedLock;
    }
}
