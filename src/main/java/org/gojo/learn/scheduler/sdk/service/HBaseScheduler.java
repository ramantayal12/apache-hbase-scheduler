package org.gojo.learn.scheduler.sdk.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import org.gojo.learn.scheduler.sdk.handler.TaskHandler;
import org.gojo.learn.scheduler.sdk.model.Task;

public class HBaseScheduler {
  private final Connection connection;
  private final ScheduledExecutorService executor;
  private final TableName tableName;
  private final Map<String, TaskHandler> handlers = new ConcurrentHashMap<>();

  // Column family and qualifiers
  private static final byte[] CF_DETAILS = Bytes.toBytes("details");
  private static final byte[] Q_TYPE = Bytes.toBytes("type");
  private static final byte[] Q_SCHEDULED = Bytes.toBytes("scheduled");
  private static final byte[] Q_INTERVAL = Bytes.toBytes("interval");
  private static final byte[] Q_STATUS = Bytes.toBytes("status");
  private static final byte[] CF_PAYLOAD = Bytes.toBytes("payload");

  public HBaseScheduler(Configuration config, String tableName) throws IOException {
    this.connection = ConnectionFactory.createConnection(config);
    this.tableName = TableName.valueOf(tableName);
    this.executor = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
    createTableIfNotExists();
  }

  private void createTableIfNotExists() throws IOException {
    try (Admin admin = connection.getAdmin()) {
      if (!admin.tableExists(tableName)) {
        HTableDescriptor table = new HTableDescriptor(tableName);
        table.addFamily(new HColumnDescriptor(CF_DETAILS));
        table.addFamily(new HColumnDescriptor(CF_PAYLOAD));
        admin.createTable(table);
      }
    }
  }

  public void registerHandler(String taskType, TaskHandler handler) {
    handlers.put(taskType, handler);
  }

  public void start() {
    executor.scheduleAtFixedRate(this::pollAndExecute, 0, 1, TimeUnit.SECONDS);
  }

  private void pollAndExecute() {
    try (Table table = connection.getTable(tableName)) {
      Scan scan = new Scan()
          .setFilter(new FilterList(
              new SingleColumnValueFilter(CF_DETAILS, Q_STATUS,
                  CompareOperator.EQUAL, Bytes.toBytes("SCHEDULED")),
              new SingleColumnValueFilter(CF_DETAILS, Q_SCHEDULED,
                  CompareOperator.LESS_OR_EQUAL, Bytes.toBytes(Instant.now().toEpochMilli()))
          ))
          .setCaching(100);

      try (ResultScanner scanner = table.getScanner(scan)) {
        for (Result result : scanner) {
          Task task = convertToTask(result);
          executor.submit(() -> executeTask(task, table));
        }
      }
    } catch (IOException e) {
      // Handle exception
    }
  }

  private void executeTask(Task task, Table table) {
    try {
      // Atomic status check-and-set
      CheckAndMutate checkAndMutate = CheckAndMutate.newBuilder(Bytes.toBytes(task.getRowKey()))
          .ifMatches(new SingleColumnValueFilter(CF_DETAILS, Q_STATUS,
              CompareOperator.EQUAL, Bytes.toBytes("SCHEDULED")))
          .build(new Put(Bytes.toBytes(task.getRowKey()))
              .addColumn(CF_DETAILS, Q_STATUS, Bytes.toBytes("RUNNING")));

      if (table.checkAndMutate(checkAndMutate).isSuccess()) {
        TaskHandler handler = handlers.get(task.getType());
        if (handler != null) {
          handler.execute(task);
          handleSuccess(task, table);
        } else {
          handleFailure(task, "No handler found", table);
        }
      }
    } catch (Exception e) {
      handleFailure(task, e.getMessage(), table);
    }
  }

  private void handleSuccess(Task task, Table table) throws IOException {
    if (task.getIntervalSeconds() > 0) {
      Put put = new Put(Bytes.toBytes(task.getRowKey()))
          .addColumn(CF_DETAILS, Q_SCHEDULED,
              Bytes.toBytes(Instant.now().plusSeconds(task.getIntervalSeconds()).toEpochMilli()))
          .addColumn(CF_DETAILS, Q_STATUS, Bytes.toBytes("SCHEDULED"));
      table.put(put);
    } else {
      Put put = new Put(Bytes.toBytes(task.getRowKey()))
          .addColumn(CF_DETAILS, Q_STATUS, Bytes.toBytes("COMPLETED"));
      table.put(put);
    }
  }

  private void handleFailure(Task task, String error, Table table) {
    try {
      Put put = new Put(Bytes.toBytes(task.getRowKey()))
          .addColumn(CF_DETAILS, Q_STATUS, Bytes.toBytes("FAILED"))
          .addColumn(CF_DETAILS, Bytes.toBytes("error"), Bytes.toBytes(error));
      table.put(put);
    } catch (IOException e) {
      // Handle exception
    }
  }

  public String scheduleTask(Task task) throws IOException {
    String rowKey = generateRowKey(task.getScheduledTime());
    task.setRowKey(rowKey);
    task.setStatus("SCHEDULED");

    Put put = new Put(Bytes.toBytes(rowKey))
        .addColumn(CF_DETAILS, Q_TYPE, Bytes.toBytes(task.getType()))
        .addColumn(CF_DETAILS, Q_SCHEDULED, Bytes.toBytes(task.getScheduledTime().toEpochMilli()))
        .addColumn(CF_DETAILS, Q_INTERVAL, Bytes.toBytes(task.getIntervalSeconds()))
        .addColumn(CF_DETAILS, Q_STATUS, Bytes.toBytes(task.getStatus()));

    // Add payload columns
    task.getPayload().forEach((k, v) ->
        put.addColumn(CF_PAYLOAD, Bytes.toBytes(k), v));

    try (Table table = connection.getTable(tableName)) {
      table.put(put);
    }
    return rowKey;
  }

  private String generateRowKey(Instant scheduledTime) {
    // Reverse timestamp for better distribution + UUID
    return String.format("%020d-%s",
        Long.MAX_VALUE - scheduledTime.toEpochMilli(),
        UUID.randomUUID());
  }

  private Task convertToTask(Result result) {
    Task task = new Task();
    task.setRowKey(Bytes.toString(result.getRow()));
    task.setType(Bytes.toString(result.getValue(CF_DETAILS, Q_TYPE)));
    task.setScheduledTime(Instant.ofEpochMilli(
        Bytes.toLong(result.getValue(CF_DETAILS, Q_SCHEDULED))));
    task.setIntervalSeconds(
        Bytes.toLong(result.getValue(CF_DETAILS, Q_INTERVAL)));
    task.setStatus(Bytes.toString(result.getValue(CF_DETAILS, Q_STATUS)));

    // Convert payload
    NavigableMap<byte[], byte[]> payloadColumns =
        result.getFamilyMap(CF_PAYLOAD);
    Map<String, byte[]> payload = new HashMap<>();
    payloadColumns.forEach((k, v) ->
        payload.put(Bytes.toString(k), v));
    task.setPayload(payload);

    return task;
  }

  public void close() throws IOException {
    executor.shutdown();
    connection.close();
  }
}
