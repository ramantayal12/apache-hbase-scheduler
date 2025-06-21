package org.gojo.learn.scheduler.sdk;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Main {

  private static final Logger log = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {
    log.info("Starting Distributed Delay Scheduler...");
    SpringApplication.run(Main.class, args);
  }
}