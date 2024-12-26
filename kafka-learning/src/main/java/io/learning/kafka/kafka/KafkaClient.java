package io.learning.kafka.kafka;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;

@Slf4j
public class KafkaClient<T> {

  private static volatile KafkaClient client = null;
  private KafkaProducer<String, String> producer;
  private KafkaConsumer<String, String> consumer;
  private KafkaProperty kafkaProperty;


  private KafkaClient(KafkaProperty kafkaProperty) {
    this.kafkaProperty = kafkaProperty;
    producer = new KafkaProducer<>(initProducerConfig());
    consumer = new KafkaConsumer<>(initConsumerConfig());
  }

  public static KafkaClient getInstance(KafkaProperty kafkaProperty) {
    if (client == null) {
      synchronized (KafkaClient.class) {
        if (client == null) {
          client = new KafkaClient(kafkaProperty);
        }
      }
    }
    return client;
  }

  public KafkaConsumer getKafkaConsumer() {
    return consumer;
  }

  public KafkaProducer getKafkaProducer() {
    return producer;
  }

  public void sendMessageWithCallback(T message) {
    ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(
        kafkaProperty.getTopic(), message.toString()
    );
    producer.send(producerRecord, (m, e) -> {
      if (e != null) {
        log.error("send kafka failed, exception={}", e);
      }
    });
  }

  public void sendMessageInSync(String message) throws ExecutionException, InterruptedException {
    ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(
        kafkaProperty.getTopic(), message
    );
    producer.send(producerRecord).get();
  }

  public void sendMessageInSync(String key,String message) throws ExecutionException, InterruptedException {
    ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(kafkaProperty.getTopic(),key, message);
    producer.send(producerRecord).get();
  }


  private Properties initProducerConfig() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperty.getBootstrapServer());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    //props.put(ProducerConfig.CLIENT_ID_CONFIG, kafkaProperty.getGroupId());
    props.put(ProducerConfig.ACKS_CONFIG, kafkaProperty.getAck());
    props.put(ProducerConfig.RETRIES_CONFIG, kafkaProperty.getRetries());
    if (kafkaProperty.isUseAuth()) {
      props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
      props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
      props.put(SaslConfigs.SASL_JAAS_CONFIG,
          "org.apache.kafka.common.security.plain.PlainLoginModule required " +
              "username=\""+kafkaProperty.getAuthUser()+"\" password=\""+kafkaProperty.getAuthPwd()+"\";");
    }
    return props;
  }

  private Properties initConsumerConfig() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperty.getBootstrapServer());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    //props.put(ConsumerConfig.CLIENT_ID_CONFIG, "bdp.sql.collection.consumer.client.id");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaProperty.getGroupId());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaProperty.getAutoOffsetReset());
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

    if (kafkaProperty.isUseAuth()) {
      props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
      props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
      props.put(SaslConfigs.SASL_JAAS_CONFIG,
          "org.apache.kafka.common.security.plain.PlainLoginModule required " +
              "username=\""+kafkaProperty.getAuthUser()+"\" password=\""+kafkaProperty.getAuthPwd()+"\";");
    }
    return props;
  }
}
