package org.gojo.learn.scheduler.sdk.handler;

import org.gojo.learn.scheduler.sdk.model.Task;
import org.springframework.stereotype.Service;

@Service
public class SchedulerTaskExecutor {

    public void execute(Task task) {
        System.out.println("Executing task: " + task.getId() + ", payload: " + task.getPayload());

        // TODO: Replace with actual business logic
        try {
            Thread.sleep(500); // Simulate work
            System.out.println("Task executed: " + task.getId());
        } catch (InterruptedException e) {
            System.err.println("Task interrupted: " + task.getId());
        }
    }

}
