package io.learning.flink.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.CommonClientConfigs;
import org.apache.flink.kafka.shaded.org.apache.kafka.common.config.SaslConfigs;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class FlinkKafkaLearning {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    env.getConfig().setGlobalJobParameters(parameterTool);
    BaseConfig baseConfig = BaseConfig.build(parameterTool);

    env.setParallelism(baseConfig.getDefaultParallelism());
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

    setCheckpointing(baseConfig, env);
    setStateBackend(baseConfig, env);

    Properties inputProps = buildKafkaConsummerProps(baseConfig);
    //kafka input topic list
    List<String> topics = new ArrayList<>();
    topics.add(baseConfig.getTopicName());
    FlinkKafkaConsumer flinkKafkaConsumer = new FlinkKafkaConsumer<String>(
        topics,
        new SimpleStringSchema(),
        inputProps);
    DataStreamSource<Tuple2<String, String>> dataStreamSource = env.addSource(flinkKafkaConsumer)
        .setParallelism(baseConfig.getKafkaSourceDefaultParallelism());
    dataStreamSource.print(); //把从 kafka 读取到的数据打印在控制台

    System.out.println("start to startup...");

    env.execute("Flink kafka learning Prog");
  }

  private static void setStateBackend(BaseConfig config, StreamExecutionEnvironment env) {
    // 设置statebackend
    StateBackend stateBackend = new FsStateBackend(config.getStateBackendFilePath());
    env.setStateBackend(stateBackend);
  }

  public static void setCheckpointing(BaseConfig config, StreamExecutionEnvironment env) {
    //设置checkpoint
    if (config.isCheckpointEnable()) {
      env.enableCheckpointing(config.getCheckpointInterval());
      // 设置模式为exactly-once （这是默认值）
      env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
      //重启策略
      // env.setRestartStrategy(RestartStrategies.noRestart());
      env.setRestartStrategy(
          RestartStrategies.fixedDelayRestart(8, Time.of(10, TimeUnit.SECONDS)));
      // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
      env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
      // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
      env.getCheckpointConfig().setCheckpointTimeout(60000);
      // 同一时间只允许进行一个检查点
      env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
      env.getCheckpointConfig().enableExternalizedCheckpoints(
          CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    }
  }

  private static Properties buildKafkaConsummerProps(BaseConfig baseConfig) {
    Properties props = new Properties();
    props.put("bootstrap.servers", baseConfig.getBootstrapServers());
    // props.put("zookeeper.connect", binCleanConfig.getKafkaZKServers());
    props.put("group.id",
        baseConfig.getGroupId());
    props.put("auto.offset.reset", "latest");
    props.put("isolation.level", "read_committed");
    // props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer1");
    // props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer1");
    if (baseConfig.isUseKafkaAuth()) {
      props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
      props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
      props.put(SaslConfigs.SASL_JAAS_CONFIG,
          "org.apache.kafka.common.security.plain.PlainLoginModule required " +
              "username=\"bdp_user\" password=\"DHBeulDD\";");
    }
    return props;
  }

}

