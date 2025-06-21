package org.gojo.learn.scheduler.sdk.hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

public class HbaseService {

    private final Connection hbaseConnection;

    @Autowired
    public HbaseService(Connection hbaseConnection) {
        this.hbaseConnection = hbaseConnection;
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
