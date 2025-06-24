package org.gojo.learn.scheduler.sdk.controller;

import org.gojo.learn.scheduler.sdk.dto.TaskDelayRequest;
import org.gojo.learn.scheduler.sdk.dto.TaskRequest;
import org.gojo.learn.scheduler.sdk.model.Task;
import org.gojo.learn.scheduler.sdk.service.TaskService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

@RestController
@RequestMapping("/schedule")
public class TaskController {

    private final TaskService taskService;

    public TaskController(TaskService taskService) {
        this.taskService = taskService;
    }

    @PostMapping("/")
    public ResponseEntity<?> schedule(@RequestBody TaskRequest req) throws IOException {
        taskService.schedule(req);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/delay")
    public ResponseEntity<?> scheduleDelay(@RequestBody TaskDelayRequest req) throws IOException {
        taskService.schedule(TaskRequest.builder()
                .type(req.getType())
                .payload(req.getPayload())
                .executeAt(Instant.now().plusSeconds(req.getDelayInSeconds()))
                .build()
        );
        return ResponseEntity.ok().build();
    }
}