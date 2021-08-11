package io.learning.flink.cdc;

import io.learning.flink.utils.CheckpointUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MysqlCdcSQLJob {

  public static void main(String[] args) {
    try {
      EnvironmentSettings mySetting = EnvironmentSettings
          .newInstance()
          .useBlinkPlanner()
          .inStreamingMode()
          .build();
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      CheckpointUtil.setConfYamlStateBackend(env);
      StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, mySetting);

      String ddl1 = ""
          + "CREATE TABLE orders (\n"
          + "  order_id INT,\n"
          + "  order_date TIMESTAMP(0),\n"
          + "  customer_name STRING,\n"
          + "  price DECIMAL(10, 5),\n"
          + "  product_id INT,\n"
          + "  order_status BOOLEAN\n"
          + ") WITH (\n"
          + "  'connector' = 'mysql-cdc',\n"
          + "  'hostname' = 'localhost',\n"
          + "  'port' = '3306',\n"
          + "  'username' = 'root',\n"
          + "  'password' = 'root',\n"
          + "  'database-name' = 'flink_sql',\n"
          + "  'table-name' = 'orders'\n"
          + ")";
      tableEnv.executeSql(ddl1);

      String ddlSink = ""
          + "CREATE TABLE kafka_gmv (\n"
          + "  day_str STRING,\n"
          + "  gmv DECIMAL(10, 5)\n"
          + ") WITH (\n"
          + "    'connector' = 'kafka',\n"
          + "    'topic' = 'kafka_gmv',\n"
          + "    'scan.startup.mode' = 'earliest-offset',\n"
          + "    'properties.bootstrap.servers' = 'localhost:9092',\n"
          + "    'format' = 'changelog-json'\n"
          + ")";
      tableEnv.executeSql(ddlSink);

      String ddlPrint = ""
          + "CREATE TABLE sink_print (\n"
          + "  day_str STRING,\n"
          + "  gmv DECIMAL(10, 5)\n"
          + ") WITH (\n"
          + "    'connector' = 'print'\n"
          + ")";
      tableEnv.executeSql(ddlPrint);


      StatementSet stmtSet = tableEnv.createStatementSet();
      stmtSet.addInsertSql(""
          + "INSERT INTO sink_print\n"
          + "SELECT DATE_FORMAT(order_date, 'yyyy-MM-dd') as day_str, SUM(price) as gmv\n"
          + "FROM orders\n"
          + "WHERE order_status = true\n"
          + "GROUP BY DATE_FORMAT(order_date, 'yyyy-MM-dd')");
      //stmtSet.addInsertSql("insert into print1_sink select 'sink1', ts from order_table");
      stmtSet.execute();

    } catch (Exception e) {
      e.printStackTrace();
      System.out.println(e.toString());
      System.err.println("任务执行失败:" + e.getMessage());
      System.exit(-1);
    }
  }


}
