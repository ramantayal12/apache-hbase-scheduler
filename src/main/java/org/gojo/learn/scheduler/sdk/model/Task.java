package org.gojo.learn.scheduler.sdk.model;

import java.time.Instant;
import java.util.Map;

public class Task {

  public String rowKey;
  public String type;
  public Instant scheduledTime;
  public long intervalSeconds;
  public Map<String, byte[]> payload;
  public String status;

  // Getters and setters

  public String getRowKey() {
    return rowKey;
  }

  public void setRowKey(String rowKey) {
    this.rowKey = rowKey;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public Instant getScheduledTime() {
    return scheduledTime;
  }

  public void setScheduledTime(Instant scheduledTime) {
    this.scheduledTime = scheduledTime;
  }

  public long getIntervalSeconds() {
    return intervalSeconds;
  }

  public void setIntervalSeconds(long intervalSeconds) {
    this.intervalSeconds = intervalSeconds;
  }

  public Map<String, byte[]> getPayload() {
    return payload;
  }

  public void setPayload(Map<String, byte[]> payload) {
    this.payload = payload;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }
}
