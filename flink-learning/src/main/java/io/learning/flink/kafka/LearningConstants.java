package io.learning.flink.kafka;

public class LearningConstants {


  public static final String USE_KAFKA_AUTH = "use-kafka-auth";

  public static final String DEFAULT_KAFKA_BROKERS = "localhost:9092";
  public static final String DEFAULT_KAFKA_ZOOKEEPER_CONNECT = "localhost:2181";
  public static final int DEFAULT_PARALLELISM_VALUE = 1;
  public static final int DEFAULT_KAFKASOURCE_PARALLELISM_VALUE = 1;


  // default topic
  public static final String DEFAULT_TOPIC = "bdp-bee-learning-topic";
  public static final String DEFAULT_STATE_BACKEND_FILE_PATH = "hdfs://localhost:9001/flink-checkpoints";



}
