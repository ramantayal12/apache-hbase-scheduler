package org.gojo.learn.scheduler.sdk.config.kafka;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Component;

@Component
public class KafkaProducerConfig {

  private final int lingerMs;
  private final int batchSize;
  private final String kafkaBootStrapAddress;

  @Autowired
  public KafkaProducerConfig(
      @Value(value = "${spring.kafka.producer.linger.ms}") int lingerMs,
      @Value(value = "${spring.kafka.producer.batch.size}") int batchSize,
      @Value(value = "${spring.kafka.bootstrap-servers}") String kafkaBootStrapAddress
  ) {
    this.lingerMs = lingerMs;
    this.batchSize = batchSize;
    this.kafkaBootStrapAddress = kafkaBootStrapAddress;
  }


  @Bean
  public ProducerFactory<String, String> defaultProducerFactory() {
    Map<String, Object> configProps = new HashMap<>();
    configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootStrapAddress);
    configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
    configProps.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
    return new DefaultKafkaProducerFactory<>(configProps);
  }

  @Bean
  public KafkaTemplate<String, String> kafkaTemplate() {
    return new KafkaTemplate<>(defaultProducerFactory());
  }
}
