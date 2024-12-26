package io.learning.flink.sql;

import java.time.ZoneId;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class HiveSQLJob {

  public static void main(String[] args) {
    try {
      final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

      EnvironmentSettings settings = EnvironmentSettings.newInstance()
          .useBlinkPlanner()
          .inStreamingMode()
          .build();
      TableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

      Catalog catalog = null;
      String catalogName = "hive";
      catalog = new MyHiveCatalog(
          catalogName,
          "default",
          "/usr/local/hive/conf");
      tEnv.registerCatalog(catalogName, catalog);

      tEnv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
      tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

      String sql = "CREATE TABLE hive.test.hive_table_xt_sql (\n"
          + "  user_name STRING\n"
          + "  ,age int\n"
          + ") PARTITIONED BY (dt STRING, hr STRING) STORED AS parquet TBLPROPERTIES (\n"
          + "  'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00',\n"
          + "  'sink.partition-commit.trigger'='partition-time',\n"
          + "  'sink.partition-commit.delay'='1 min',\n"
          + "  'sink.partition-commit.policy.kind'='metastore,success-file'\n"
          + ")";
      tEnv.executeSql(sql);

    } catch (Exception e) {
      e.printStackTrace();
      System.out.println(e.toString());
      System.err.println("任务执行失败:" + e.getMessage());
      System.exit(-1);
    }
  }
}
