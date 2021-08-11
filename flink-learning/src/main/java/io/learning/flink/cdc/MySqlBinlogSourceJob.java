package io.learning.flink.cdc;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class MySqlBinlogSourceJob {

  public static void main(String[] args) throws Exception {
    SourceFunction<String> sourceFunction = MySQLSource.<String>builder()
        .hostname("localhost")
        .port(3306)
        .databaseList("flink_sql") // monitor all tables under inventory database
        .username("root")
        .password("root")
        .tableList("flink_sql.orders")
        .startupOptions(StartupOptions.latest())
        .deserializer(new StringDebeziumDeserializationSchema()) // converts SourceRecord to String
        .build();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env
        .addSource(sourceFunction)
        .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering
    env.execute();
  }

}
