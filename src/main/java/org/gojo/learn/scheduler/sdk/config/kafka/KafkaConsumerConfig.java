package org.gojo.learn.scheduler.sdk.config.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConsumerAwareRecordRecoverer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumerConfig {

  private static final Logger LOGGER = Logger.getLogger(KafkaConsumerConfig.class.getName());

  private final int maxPartitionFetchBytes;
  private final int maxPollRecords;
  private final int fetchMaxBytes;
  private final int fetchMinBytes;
  private final String defaultGroupID;
  private final int maxPollInterval;
  private final int heartBeatInterval;
  private final int sessionTimeout;
  private final String kafkaBootstrapAddress;
  private final int kafkaConcurrency;

  @Autowired
  public KafkaConsumerConfig(
      @Value(value = "${spring.kafka.consumer.max.partition.fetch.bytes}") int maxPartitionFetchBytes,
      @Value(value = "${spring.kafka.consumer.max.poll.records}") int maxPollRecords,
      @Value(value = "${spring.kafka.consumer.fetch.max.bytes}") int fetchMaxBytes,
      @Value(value = "${spring.kafka.consumer.fetch.min.bytes}") int fetchMinBytes,
      @Value(value = "${spring.kafka.consumer.group-id.default}") String defaultGroupID,
      @Value(value = "${spring.kafka.consumer.max.poll.interval.ms}") int maxPollInterval,
      @Value(value = "${spring.kafka.consumer.heartbeat.ms}") int heartBeatInterval,
      @Value(value = "${spring.kafka.consumer.session.timeout.ms}") int sessionTimeout,
      @Value(value = "${spring.kafka.bootstrap-servers}") String kafkaBootstrapAddress,
      @Value(value = "${spring.kafka.consumer.concurrency.default}") int kafkaConcurrency
  ) {
    this.maxPartitionFetchBytes = maxPartitionFetchBytes;
    this.maxPollRecords = maxPollRecords;
    this.fetchMaxBytes = fetchMaxBytes;
    this.fetchMinBytes = fetchMinBytes;
    this.defaultGroupID = defaultGroupID;
    this.maxPollInterval = maxPollInterval;
    this.heartBeatInterval = heartBeatInterval;
    this.sessionTimeout = sessionTimeout;
    this.kafkaBootstrapAddress = kafkaBootstrapAddress;
    this.kafkaConcurrency = kafkaConcurrency;
  }

  public ConsumerFactory<String, String> consumerFactory() {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapAddress);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, defaultGroupID);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeout);
    props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollInterval);
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
    props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, fetchMaxBytes);
    props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, fetchMinBytes);
    props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartBeatInterval);
    props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes);
    return new DefaultKafkaConsumerFactory<>(
        props,
        new StringDeserializer(),
        new StringDeserializer()
    );
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {

    ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory());
    factory.setConcurrency(kafkaConcurrency);
    factory.setAutoStartup(true);
    factory.getContainerProperties().setMicrometerEnabled(true);
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
    factory.setCommonErrorHandler(new DefaultErrorHandler(dlqErrorHandler()));

    return factory;
  }

  public ConsumerAwareRecordRecoverer dlqErrorHandler() {
    return (consumerRecord, consumer, thrownException) -> LOGGER.info("DLQ error handler");
  }

}
