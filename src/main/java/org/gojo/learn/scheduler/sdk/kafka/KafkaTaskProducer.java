package org.gojo.learn.scheduler.sdk.kafka;

import org.gojo.learn.scheduler.sdk.model.Task;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaTaskProducer {
    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaTaskProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishTask(Task task) {
        kafkaTemplate.send("scheduled-tasks", task.getId(), task.getPayload());
    }
}