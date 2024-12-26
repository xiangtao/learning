package io.learning.flink.sql;

import io.learning.flink.sql.sink.ComplexSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author fanrui03
 * @date 2021/5/15 14:22
 */
public class BackPressureDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings mySetting = EnvironmentSettings
            .newInstance()
            .useBlinkPlanner()
            .inStreamingMode()
            .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, mySetting);

        String sourceDDL = "CREATE TABLE orders (\n" +
                "  app          INT,\n" +
                "  channel      INT,\n" +
                "  user_id      STRING,\n" +
                "  ts           TIMESTAMP(3),\n" +
                "  WATERMARK FOR ts AS ts\n" +
                ") WITH (\n" +
                "   'connector' = 'datagen',\n" +
                "   'rows-per-second'='10000',\n" +
                "   'fields.app.min'='1',\n" +
                "   'fields.app.max'='10',\n" +
                "   'fields.channel.min'='21',\n" +
                "   'fields.channel.max'='30',\n" +
                "   'fields.user_id.length'='10'\n" +
                ")";

        tableEnv.executeSql(sourceDDL);

        Table query = tableEnv.sqlQuery("select * from orders");
        tableEnv.toAppendStream(query, Row.class)
                .rebalance()
//                .addSink(new RequestHBaseSink())
                .addSink(new ComplexSink())
                .name("MySink");

        System.out.println(env.getExecutionPlan());

        env.execute(BackPressureDemo.class.getSimpleName());
    }

}
