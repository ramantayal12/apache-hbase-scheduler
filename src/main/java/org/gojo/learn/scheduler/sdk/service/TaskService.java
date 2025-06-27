package org.gojo.learn.scheduler.sdk.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.gojo.learn.scheduler.sdk.dto.TaskRequest;
import org.gojo.learn.scheduler.sdk.hbase.HbaseService;
import org.gojo.learn.scheduler.sdk.kafka.KafkaTaskProducer;
import org.gojo.learn.scheduler.sdk.model.Task;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
public class TaskService {

    private final HbaseService hbaseService;
    private final ObjectMapper objectMapper;
    private final KafkaTaskProducer kafkaTaskProducer;

    @Autowired
    public TaskService(
            HbaseService hbaseService,
            ObjectMapper objectMapper,
            KafkaTaskProducer kafkaTaskProducer
    ) {
        this.hbaseService = hbaseService;
        this.objectMapper = objectMapper;
        this.kafkaTaskProducer = kafkaTaskProducer;
    }


    public void schedule(TaskRequest req) throws IOException {
        Task task = Task.builder()
                .id(UUID.randomUUID().toString())
                .type(req.getType())
                .executeAt(req.getExecuteAt())
                .payload(req.getPayload())
                .build();

        String value = objectMapper.writeValueAsString(task);

        hbaseService.saveData(
                req.getExecuteAt().toEpochMilli(),
                "FirstColumn",
                value
        );
    }

    public List<Task> fetchTasksForNext60Seconds() throws IOException {
        long now = Instant.now().toEpochMilli();
        long end = now + 60000;  // 60 seconds in milliseconds

        return hbaseService.fetchData(now, end);
    }

    @Scheduled(fixedRate = 60000)  // Run every 30 seconds
    public void processScheduledTasks() {
        try {
            // Fetch tasks for next 60 seconds
            List<Task> upcomingTasks = fetchTasksForNext60Seconds();

            // Process each task
            for (Task task : upcomingTasks) {
                executeTask(task);
            }
        } catch (IOException e) {
            log.error("Error fetching scheduled tasks", e);
        }
    }

    private void executeTask(Task task) {
        kafkaTaskProducer.publishTask(task);
        log.info("Publishing task to kafka: {}", task.getId());
    }

}
