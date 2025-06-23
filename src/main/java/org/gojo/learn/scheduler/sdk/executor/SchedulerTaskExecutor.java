package org.gojo.learn.scheduler.sdk.executor;

import lombok.extern.slf4j.Slf4j;
import org.gojo.learn.scheduler.sdk.model.Task;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class SchedulerTaskExecutor {

    public void execute(Task task) {
        log.info("Executing task: " + task.getId() + ", payload: " + task.getPayload());
    }

}
