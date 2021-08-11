package io.learning.flink.sink;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Before;
import org.junit.Test;

/**
 * @author xiexinyuan
 * @Description
 * @date 2021/4/26
 * @time 4:50 下午
 */
public class TestRocketMQTableSink {

    StreamExecutionEnvironment env;
    StreamTableEnvironment tableEnv;


    @Before
    public void createEnv(){
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.enableCheckpointing(1000 * 10, CheckpointingMode.AT_LEAST_ONCE);

        //创建旧tableEnv
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        tableEnv = StreamTableEnvironment.create(env, settings);
        tableEnv.getConfig().getConfiguration().setString("table.dynamic-table-options.enabled","true");
    }

    @Test
    public void testRocketMQSinkCase1(){
        String source = "CREATE TABLE datagen (\n" +
                " id INT,\n" +
                " user_id INT,\n" +
                " lesson_id INT,\n" +
                " user_name STRING, \n" +
                " ts AS localtimestamp\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='1000',\n" +
                " 'fields.id.kind'='sequence',\n" +
                " 'fields.id.start'='1',\n" +
                " 'fields.id.end'='10000',\n" +
                " 'fields.user_id.min'='1',\n" +
                " 'fields.user_id.max'='1000',\n" +
                " 'fields.lesson_id.min'='100',\n" +
                " 'fields.lesson_id.max'='10000',\n" +
                " 'fields.user_name.length'='10'\n" +
                ")";

        String sink = "CREATE TABLE `rocket_sink_table` (\n" +
                "  `id` INT ,\n" +
                "  `user_id` INT,\n" +
                "  `lesson_id` INT,\n" +
                "  `user_name` STRING, \n" +
                "  `ts` TIMESTAMP\n" +
                ") WITH (    \n" +
                "     'connector' = 'koolearn-rocketmq',\n" +
                "     'topic' = 'bigdata_test_rocketmq_connector_topic',\n" +
                "     'nameserver' = 'lutra.neibu.koolearn.com:80',\n" +
                "     'group'='bigdata-test-rocketmq-connector-topic-producer',\n" +
                "     'tag'='tag1',\n" +
                "     'product_send_async'='false',\n" +
                "     'product_send_batch'='3000',\n" +
                "     'product_send_interval' = '2000',\n" +
                "     'format' = 'json')";

//        String sink = "CREATE TABLE `rocket_sink_table` (\n" +
//                "  `id` INT ,\n" +
//                "  `user_id` INT,\n" +
//                "  `lesson_id` INT,\n" +
//                "  `ts` TIMESTAMP\n" +
//                ") WITH (    \n" +
//                "     'connector' = 'print')";

        String insertSql = "insert into rocket_sink_table" +
                " select id,user_id,lesson_id,user_name,ts " +
                "from datagen";

        tableEnv.executeSql(source);
        tableEnv.executeSql(sink);
        TableResult tableResult = tableEnv.executeSql(insertSql);
        tableResult.print();
     }

}
