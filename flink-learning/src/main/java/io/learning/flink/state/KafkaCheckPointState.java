package io.learning.flink.state;

import java.util.Properties;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class KafkaCheckPointState {

  public static void main(String[] args) {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    //设置并行度
    env.setParallelism(1);
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

    //设置checkpoint   1分钟
    env.enableCheckpointing(60000);

    // 设置模式为exactly-once （这是默认值）
    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    //重启策略
    env.setRestartStrategy(RestartStrategies.noRestart());
    // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
    // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
    env.getCheckpointConfig().setCheckpointTimeout(10000);
    // 同一时间只允许进行一个检查点
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

    //设置statebackend
    try {
      //StateBackend stateBackend = new RocksDBStateBackend(
      // "hdfs://node1.hadoop:9000/flink/checkpoint", true);
      StateBackend stateBackend = new FsStateBackend("file:///data/store");
      env.setStateBackend(stateBackend);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint【详细解释见备注】
    //ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION:表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
    //ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION: 表示一旦Flink处理程序被cancel后，会删除Checkpoint数据，只有job执行失败的时候才会保存checkpoint
    env.getCheckpointConfig().enableExternalizedCheckpoints(
        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

    //todo 获取kafka的配置属性

    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    //props.put("zookeeper.connect", "localhost:2181");
    props.put("group.id", "metric-group");
    //props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  //key 反序列化
    //props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("auto.offset.reset", "latest"); //value 反序列化

    DataStream<String> kafkaDstream = env.addSource(new FlinkKafkaConsumer<String>(
        "topic-input",
        new SimpleStringSchema(),
        props).setStartFromGroupOffsets()
    );
    DataStream<String> mapDstream = kafkaDstream.keyBy(x -> x).map(
        new RichMapFunction<String, String>() {
          private ValueState<Tuple2<String, Integer>> valueState;

          @Override
          public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 创建 ValueStateDescriptor
            ValueStateDescriptor descriptor = new ValueStateDescriptor("kafkaCheckPointState",
                TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                }));
            // 激活 StateTTL
            // descriptor.enableTimeToLive(ttlConfig);
            // 基于 ValueStateDescriptor 创建 ValueState
            valueState = getRuntimeContext().getState(descriptor);
            System.out.println(valueState.getClass());
          }

          @Override
          public String map(String s) throws Exception {
            Tuple2<String, Integer> currentState = valueState.value();

            if (currentState == null) {
              System.out.println("上次状态=" + null);
              currentState = new Tuple2<>(s, 1);
              valueState.update(currentState);
            } else {
              System.out.println("上次状态=" + currentState);
              currentState = new Tuple2<>(currentState.f0, currentState.f1 + 1);
              valueState.update(currentState);
            }
            return "日志：" + currentState;
          }
        });
    mapDstream.print();
    try {
      env.execute("startExecute");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
