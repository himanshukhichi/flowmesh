package com.flowmesh.timeline;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/runs")
public class DagRunTimelineController {
    private final TaskStateTransitionRepository transitionRepository;

    public DagRunTimelineController(TaskStateTransitionRepository transitionRepository) {
        this.transitionRepository = transitionRepository;
    }

    @GetMapping("/{dagRunId}/timeline")
    public List<TaskTimelineEvent> timeline(@PathVariable UUID dagRunId) {
        return transitionRepository.findByDagRunIdOrderByTransitionedAtAsc(dagRunId).stream()
                .map(TaskTimelineEvent::from)
                .toList();
    }
}
