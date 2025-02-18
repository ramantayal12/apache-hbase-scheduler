package org.gojo.learn.scheduler.sdk;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.gojo.learn.scheduler.sdk.model.Task;
import org.gojo.learn.scheduler.sdk.service.HBaseScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static final Logger log = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) throws IOException {
    Configuration config = HBaseConfiguration.create();
    config.set("homebrew.mxcl.hbase", "localhost");

    HBaseScheduler scheduler = new HBaseScheduler(config, "scheduler_tasks");

    scheduler.registerHandler("report", task1 -> {
      log.info("Generating report with params: " +
          task1.getPayload().entrySet().stream()
              .collect(Collectors.toMap(
                  Map.Entry::getKey,
                  e -> new String(e.getValue()))
              ));
    });

    // Schedule a task
    Task task = new Task();
    task.setType("report");
    task.setScheduledTime(Instant.now().plusSeconds(60));
    task.setIntervalSeconds(1); // Repeat hourly
    task.setPayload(Map.of(
        "format", "pdf".getBytes(),
        "recipient", "admin@company.com".getBytes()
    ));

    scheduler.scheduleTask(task);
    scheduler.start();
  }
}
