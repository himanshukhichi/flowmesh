package com.flowmesh.scheduler.api;

import com.flowmesh.scheduler.pause.SchedulingPauseService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/admin/scheduler")
public class AdminSchedulerController {
    private final SchedulingPauseService pauseService;

    public AdminSchedulerController(SchedulingPauseService pauseService) {
        this.pauseService = pauseService;
    }

    @PostMapping("/pause")
    public SchedulingPauseService.PauseStatus pauseAll() {
        pauseService.pauseAll();
        return pauseService.status(null);
    }

    @PostMapping("/resume")
    public SchedulingPauseService.PauseStatus resumeAll() {
        pauseService.resumeAll();
        return pauseService.status(null);
    }

    @PostMapping("/dags/{dagId}/pause")
    public SchedulingPauseService.PauseStatus pauseDag(@PathVariable String dagId) {
        pauseService.pauseDag(dagId);
        return pauseService.status(dagId);
    }

    @PostMapping("/dags/{dagId}/resume")
    public SchedulingPauseService.PauseStatus resumeDag(@PathVariable String dagId) {
        pauseService.resumeDag(dagId);
        return pauseService.status(dagId);
    }

    @GetMapping("/dags/{dagId}/pause")
    public SchedulingPauseService.PauseStatus pauseStatus(@PathVariable String dagId) {
        return pauseService.status(dagId);
    }
}
