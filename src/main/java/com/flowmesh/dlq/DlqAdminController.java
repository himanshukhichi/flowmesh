package com.flowmesh.dlq;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/admin/dlq")
public class DlqAdminController {
    private final DlqService dlqService;

    public DlqAdminController(DlqService dlqService) {
        this.dlqService = dlqService;
    }

    @GetMapping
    public List<DlqTaskResponse> inspect() {
        return dlqService.inspect();
    }

    @PostMapping("/{dlqTaskId}/requeue")
    public DlqTaskResponse requeue(@PathVariable UUID dlqTaskId) {
        return dlqService.requeue(dlqTaskId);
    }
}
