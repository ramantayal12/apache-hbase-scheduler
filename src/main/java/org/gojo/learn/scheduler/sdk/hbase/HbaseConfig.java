package org.gojo.learn.scheduler.sdk.hbase;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

@Configuration
public class HbaseConfig {

    private final String zookeeperQuorum;
    private final String clientPort;

    public HbaseConfig(
            @Value("${hbase.zookeeper.quorum}") String zookeeperQuorum,
            @Value("${hbase.zookeeper.property.clientPort}") String clientPort) {
        this.zookeeperQuorum = zookeeperQuorum;
        this.clientPort = clientPort;
    }

    @Bean
    public Connection hbaseConnection() throws IOException {
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zookeeperQuorum);
        config.set("hbase.zookeeper.property.clientPort", clientPort);
        return ConnectionFactory.createConnection(config);
    }

}
