package org.gojo.learn.scheduler.sdk.kafka;

import org.gojo.learn.scheduler.sdk.model.Task;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaTaskProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String scheduledTasksTopic;

    public KafkaTaskProducer(
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${queue.kafka-topic}") String scheduledTasksTopic
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.scheduledTasksTopic = scheduledTasksTopic;
    }

    public void publishTask(Task task) {
        kafkaTemplate.send(scheduledTasksTopic, task.getId(), task.getPayload());
    }
}