package com.flowmesh.scheduler.pause;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

@Service
public class SchedulingPauseService {
    private static final String PAUSE_ALL_KEY = "flowmesh:scheduler:pause:all";
    private static final String PAUSE_DAG_PREFIX = "flowmesh:scheduler:pause:dag:";

    private final StringRedisTemplate redisTemplate;

    public SchedulingPauseService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void pauseAll() {
        redisTemplate.opsForValue().set(PAUSE_ALL_KEY, "true");
    }

    public void resumeAll() {
        redisTemplate.delete(PAUSE_ALL_KEY);
    }

    public void pauseDag(String dagId) {
        redisTemplate.opsForValue().set(PAUSE_DAG_PREFIX + dagId, "true");
    }

    public void resumeDag(String dagId) {
        redisTemplate.delete(PAUSE_DAG_PREFIX + dagId);
    }

    public boolean isPaused(String dagId) {
        return Boolean.TRUE.equals(redisTemplate.hasKey(PAUSE_ALL_KEY))
                || Boolean.TRUE.equals(redisTemplate.hasKey(PAUSE_DAG_PREFIX + dagId));
    }

    public PauseStatus status(String dagId) {
        return new PauseStatus(
                Boolean.TRUE.equals(redisTemplate.hasKey(PAUSE_ALL_KEY)),
                dagId == null ? false : Boolean.TRUE.equals(redisTemplate.hasKey(PAUSE_DAG_PREFIX + dagId))
        );
    }

    public record PauseStatus(boolean allPaused, boolean dagPaused) {
    }
}
