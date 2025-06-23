package org.gojo.learn.scheduler.sdk.hbase;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.gojo.learn.scheduler.sdk.model.Task;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
public class HbaseService {

    private final ObjectMapper objectMapper;
    private final Connection hbaseConnection;

    @Autowired
    public HbaseService(
            ObjectMapper objectMapper,
            Connection hbaseConnection
    ) {
        this.objectMapper = objectMapper;
        this.hbaseConnection = hbaseConnection;
    }

    public void createColumnFamily(String tableNameStr, String columnFamilyName) throws IOException {

        Admin admin = hbaseConnection.getAdmin();
        TableName tableName = TableName.valueOf(tableNameStr);

        if (!admin.tableExists(tableName)) {
            // Define column family
            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(columnFamilyName))
                    .build();

            // Create table
            admin.createTable(tableDescriptor);
            log.info("Table created: " + tableNameStr);
        } else {
            log.info("Table already exists: " + tableNameStr);
        }

    }


    public void saveData(
            String tableName,
            long rowKey,
            String columnFamily,
            String columnQualifier,
            String value
    ) throws IOException {

        try (Table table = hbaseConnection.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(
                    Bytes.toBytes(columnFamily),
                    Bytes.toBytes(columnQualifier),
                    Bytes.toBytes(value)
            );
            table.put(put);
        }
    }

    public String fetchData(
            String tableName,
            long rowKey,
            String columnFamily,
            String columnQualifier
    ) throws IOException {

        try (Table table = hbaseConnection.getTable(TableName.valueOf(tableName))) {
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifier));

            Result result = table.get(get);
            byte[] value = result.getValue(
                    Bytes.toBytes(columnFamily),
                    Bytes.toBytes(columnQualifier)
            );

            return value != null ? Bytes.toString(value) : null;
        }
    }

    public List<Task> fetchData(long rowKeyStart, long rowKeyEnd, String tableName ,String coloumnFamilyName) throws IOException {

        Scan scan = new Scan()
                .withStartRow(Bytes.toBytes(rowKeyStart))
                .withStopRow(Bytes.toBytes(rowKeyEnd))
                .addFamily(Bytes.toBytes(coloumnFamilyName));

        try (Table table = hbaseConnection.getTable(TableName.valueOf(tableName));
             ResultScanner scanner = table.getScanner(scan)) {

            List<Task> tasks = new ArrayList<>();
            for (Result result : scanner) {
                for (Cell cell : result.listCells()) {
                    // Extract task JSON from cell value
                    String taskJson = Bytes.toString(
                            cell.getValueArray(),
                            cell.getValueOffset(),
                            cell.getValueLength()
                    );

                    // Deserialize to Task object
                    Task task = objectMapper.readValue(taskJson, Task.class);
                    tasks.add(task);
                }
            }
            return tasks;
        }

    }
}
