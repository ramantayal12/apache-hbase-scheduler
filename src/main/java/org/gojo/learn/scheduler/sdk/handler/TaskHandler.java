package org.gojo.learn.scheduler.sdk.handler;

import org.gojo.learn.scheduler.sdk.model.Task;

public interface TaskHandler {

  void execute(Task task) throws Exception;
}
