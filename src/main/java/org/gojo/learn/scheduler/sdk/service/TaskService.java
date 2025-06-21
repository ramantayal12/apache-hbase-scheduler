package org.gojo.learn.scheduler.sdk.service;

import org.gojo.learn.scheduler.sdk.dto.TaskRequest;
import org.gojo.learn.scheduler.sdk.kafka.KafkaTaskProducer;
import org.gojo.learn.scheduler.sdk.model.Task;
import org.gojo.learn.scheduler.sdk.repository.TaskRepository;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class TaskService {

    private final TaskRepository taskRepo;

    public TaskService(TaskRepository taskRepo) {
        this.taskRepo = taskRepo;
    }

    public Task schedule(TaskRequest req) {
        Task task = Task.builder()
                .id(UUID.randomUUID().toString())
                .type(req.getType())
                .executeAt(req.getExecuteAt())
                .payload(req.getPayload())
                .build();

        return taskRepo.save(task);
    }
}
