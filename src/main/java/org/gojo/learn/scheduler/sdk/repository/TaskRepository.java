package org.gojo.learn.scheduler.sdk.repository;

import org.gojo.learn.scheduler.sdk.model.Task;
import org.gojo.learn.scheduler.sdk.model.TaskStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.Instant;
import java.util.List;

public interface TaskRepository extends JpaRepository<Task, String> {

    List<Task> findByExecuteAtBeforeAndStatus(Instant time, TaskStatus status);

}
