package org.gojo.learn.scheduler.sdk.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.gojo.learn.scheduler.sdk.executor.SchedulerTaskExecutor;
import org.gojo.learn.scheduler.sdk.model.Task;
import org.gojo.learn.scheduler.sdk.model.TaskStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Instant;

@Service
public class KafkaTaskConsumer {

    private final ObjectMapper mapper;
    private final SchedulerTaskExecutor schedulerTaskExecutor;

    @Autowired
    public KafkaTaskConsumer(
            ObjectMapper mapper,
            SchedulerTaskExecutor schedulerTaskExecutor
    ) {
        this.mapper = mapper;
        this.schedulerTaskExecutor = schedulerTaskExecutor;
    }

    @KafkaListener(
            topics = "scheduled-tasks",
            groupId = "task-scheduler"
    )
    public void consume(String message) throws IOException {
        Task task = mapper.readValue(message, Task.class);
        Instant now = Instant.now();

        try {
            schedulerTaskExecutor.execute(task);
            task.setStatus(TaskStatus.SUCCESS);
        } catch (Exception e) {
            e.printStackTrace();
            task.setStatus(TaskStatus.FAILED);
        }

    }
}

