package io.learning.flink.kafka;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;

@Data
public class BaseConfig {

  public static final int BATCH_SEND_SIZE = 20000;
  public static final int BATCH_SEND_INTERVAL = 1000 * 60 * 1;

  private ParameterTool parameterTool = null;

  /**
   * common parallelism config
   */
  private int defaultParallelism = LearningConstants.DEFAULT_PARALLELISM_VALUE;
  private int kafkaSourceDefaultParallelism = LearningConstants.DEFAULT_KAFKASOURCE_PARALLELISM_VALUE;
  /**
   * state
   */
  private String stateBackendFilePath = LearningConstants.DEFAULT_STATE_BACKEND_FILE_PATH;
  private boolean checkpointEnable = true;
  private long checkpointInterval = 120000;

  /**
   * kafka
   */
  private String bootstrapServers = LearningConstants.DEFAULT_KAFKA_BROKERS;
  private String kafkaZKServers = LearningConstants.DEFAULT_KAFKA_ZOOKEEPER_CONNECT;
  private String groupId = "bdp-bee-learning-group";
  private String scanStartupMode;
  private String scanStartupSpecificOffsets;
  private long scanStartupTimestampMillis;
  private boolean useKafkaAuth = false;

  private String topicName = LearningConstants.DEFAULT_TOPIC;


  public static BaseConfig build(ParameterTool parameterTool) {
    BaseConfig config = new BaseConfig();
    config.setParameterTool(parameterTool);
    if(StringUtils.isNotBlank(parameterTool.get("stateBackendFilePath"))){
      config.setStateBackendFilePath(parameterTool.get("stateBackendFilePath"));
    }
    if(StringUtils.isNotBlank(parameterTool.get("bootstrapServers"))){
      config.setBootstrapServers(parameterTool.get("bootstrapServers"));
    }
    if(StringUtils.isNotBlank(parameterTool.get("kafkaZKServers"))){
      config.setKafkaZKServers(parameterTool.get("kafkaZKServers"));
    }
    if(StringUtils.isNotBlank(parameterTool.get("topicName"))){
      config.setTopicName(parameterTool.get("topicName"));
    }
    return config;
  }
}
