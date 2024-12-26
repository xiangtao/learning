package io.learning.flink.cdc;

import io.learning.flink.utils.CheckpointUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * cdc 2.0 demo
 */
public class MysqlCdcSQLJobV2 {

  public static void main(String[] args) {

    demo1();
  }

  private static void demo1() {
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
          + "  order_id INT primary key not enforced,\n"
          + "  order_date TIMESTAMP(3),\n"
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
          + "  'table-name' = 'orders2'\n"
          + ")";
      tableEnv.executeSql(ddl1);

      String ddlPrint = ""
          + "CREATE TABLE sink_print (\n"
          + "  order_id INT primary key not enforced,\n"
          + "  order_date TIMESTAMP(3),\n"
          + "  customer_name STRING,\n"
          + "  price DECIMAL(10, 5),\n"
          + "  product_id INT,\n"
          + "  order_status BOOLEAN\n"
          + ") WITH (\n"
          + "    'connector' = 'print'\n"
          + ")";
      tableEnv.executeSql(ddlPrint);
      StatementSet stmtSet = tableEnv.createStatementSet();
      stmtSet.addInsertSql(""
          + "INSERT INTO sink_print\n"
          + "SELECT * \n"
          + "FROM orders\n");

      stmtSet.execute();

    } catch (Exception e) {
      e.printStackTrace();
      System.out.println(e.toString());
      System.err.println("任务执行失败:" + e.getMessage());
      System.exit(-1);
    }
  }

  private static void demo2() {
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
          + "  order_id INT primary key not enforced,\n"
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
          + "  'table-name' = 'orders2'\n"
          + ")";
      tableEnv.executeSql(ddl1);

      String ddlPrint2 = ""
          + "CREATE TABLE sink_print2 (\n"
          + "  order_date string primary key not enforced,\n"
          + "  sum_price DECIMAL(10, 5)\n"
          + ") WITH (\n"
          + "    'connector' = 'print'\n"
          + ")";
      tableEnv.executeSql(ddlPrint2);
      String sql2 = "insert into sink_print2 "
          + "select DATE_FORMAT(order_date,'yyyyMMdd'),sum(price) from orders group by  DATE_FORMAT(order_date,'yyyyMMdd') ";

      StatementSet stmtSet = tableEnv.createStatementSet();
      stmtSet.addInsertSql(sql2);
      String explain = stmtSet.explain();
      System.out.println(explain);
      stmtSet.execute();

    } catch (Exception e) {
      e.printStackTrace();
      System.out.println(e.toString());
      System.err.println("任务执行失败:" + e.getMessage());
      System.exit(-1);
    }
  }


}
