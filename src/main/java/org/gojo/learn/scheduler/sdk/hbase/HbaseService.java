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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
public class HbaseService {

    private final ObjectMapper objectMapper;
    private final Connection hbaseConnection;

    private final String tableName;
    private final String columnFamilyName;

    @Autowired
    public HbaseService(
            ObjectMapper objectMapper,
            Connection hbaseConnection,
            @Value("${database.table-name}") String tableName,
            @Value("${database.column-family}") String columnFamilyName
    ) {
        this.objectMapper = objectMapper;
        this.hbaseConnection = hbaseConnection;
        this.tableName = tableName;
        this.columnFamilyName = columnFamilyName;
    }

    public void createColumnFamily(
            String tableNameStr,
            String columnFamilyName
    ) throws IOException {

        Admin admin = hbaseConnection.getAdmin();
        TableName table = TableName.valueOf(tableNameStr);

        if (!admin.tableExists(table)) {
            // Define column family
            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(table)
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
            long rowKey,
            String columnQualifier,
            String value
    ) throws IOException {

        try (Table table = hbaseConnection.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(
                    Bytes.toBytes(columnFamilyName),
                    Bytes.toBytes(columnQualifier),
                    Bytes.toBytes(value)
            );
            table.put(put);
        }
    }

    public String fetchData(
            long rowKey,
            String columnQualifier
    ) throws IOException {

        try (Table table = hbaseConnection.getTable(TableName.valueOf(tableName))) {
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnQualifier));

            Result result = table.get(get);
            byte[] value = result.getValue(
                    Bytes.toBytes(columnFamilyName),
                    Bytes.toBytes(columnQualifier)
            );

            return value != null ? Bytes.toString(value) : null;
        }
    }

    public List<Task> fetchData(
            long rowKeyStart,
            long rowKeyEnd
    ) throws IOException {

        Scan scan = new Scan()
                .withStartRow(Bytes.toBytes(rowKeyStart))
                .withStopRow(Bytes.toBytes(rowKeyEnd))
                .addFamily(Bytes.toBytes(columnFamilyName));

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
