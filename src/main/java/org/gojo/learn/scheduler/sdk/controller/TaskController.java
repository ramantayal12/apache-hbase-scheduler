package org.gojo.learn.scheduler.sdk.controller;

import org.gojo.learn.scheduler.sdk.dto.TaskRequest;
import org.gojo.learn.scheduler.sdk.model.Task;
import org.gojo.learn.scheduler.sdk.service.TaskService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/schedule")
public class TaskController {
    private final TaskService taskService;

    public TaskController(TaskService taskService) {
        this.taskService = taskService;
    }

    @PostMapping
    public ResponseEntity<?> schedule(@RequestBody TaskRequest req) {
        Task task = taskService.schedule(req);
        return ResponseEntity.ok(Map.of("taskId", task.getId()));
    }
}