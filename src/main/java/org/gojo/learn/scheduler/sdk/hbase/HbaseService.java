package org.gojo.learn.scheduler.sdk.hbase;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Slf4j
@Service
public class HbaseService {

    private final Connection hbaseConnection;

    @Autowired
    public HbaseService(Connection hbaseConnection) {
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
            String rowKey,
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
            String rowKey,
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
}
