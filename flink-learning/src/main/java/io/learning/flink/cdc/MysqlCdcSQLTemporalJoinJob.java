package io.learning.flink.cdc;

import io.learning.flink.utils.CheckpointUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * cdc 2.0 时态表join
 */
public class MysqlCdcSQLTemporalJoinJob {

    public static void main(String[] args) {
        demo1();
    }

    /**
     * cdc table lookup join mysql table (当前支持处理时间的join)
     */
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

            String ddlOrder = ""
                + "CREATE TABLE orders (\n"
                + "   order_number INT,\n"
                + "   order_date TIMESTAMP(0),\n"
                + "   purchaser INT,\n"
                + "   quantity INT,\n"
                + "   product_id INT,\n"
                + "   proc_time as procTime(),"
                + "   PRIMARY KEY (order_number) NOT ENFORCED\n"
                + " ) WITH (\n"
                + "   'connector' = 'mysql-cdc',\n"
                + "   'hostname' = 'localhost',\n"
                + "   'port' = '3306',\n"
                + "   'username' = 'root',\n"
                + "   'password' = 'root',\n"
                + "   'database-name' = 'flink_sql',\n"
                + "   'table-name' = 'orders',\n"
                + "   'debezium.database.history.instance.name' = 'orders'\n"
                + " )";
            tableEnv.executeSql(ddlOrder);

            String ddlProduct = ""
                + "CREATE TABLE `products` (\n"
                + "  `id` INT,\n"
                + "  `name` STRING,\n"
                + "  `description` STRING,\n"
                + "  `price` decimal(10,2),\n"
                + "   PRIMARY KEY (id) NOT ENFORCED\n"
                + " ) WITH (\n"
                + "    'connector' = 'jdbc',\n"
                + "    'url' = 'jdbc:mysql://localhost:3306/flink_sql',\n"
                + "    'username'='root',\n"
                + "    'password'='root',\n"
                + "    'table-name' = 'products'"
                + " )";
            tableEnv.executeSql(ddlProduct);

            String printSink = ""
                + "CREATE TABLE enriched_orders_print (\n"
                + "   order_number INT,\n"
                + "   order_date TIMESTAMP(0),\n"
                + "   quantity INT,\n"
                + "   product_id INT,\n"
                + "   product_name STRING,\n"
                + "   product_price DECIMAL(10, 2),\n"
                + "   PRIMARY KEY (order_number) NOT ENFORCED\n"
                + " ) WITH (\n"
                + "     'connector' = 'print'\n"
                + " )";
            tableEnv.executeSql(printSink);

            StatementSet stmtSet = tableEnv.createStatementSet();
            stmtSet.addInsertSql(""
                + "INSERT INTO enriched_orders_print\n"
                + " SELECT \n"
                + "    o.order_number,\n"
                + "    o.order_date,\n"
                + "    o.quantity,\n"
                + "    p.id AS product_id,\n"
                + "    p.name AS product_name,\n"
                + "    p.price AS product_price \n"
                + " FROM\n"
                + "    orders AS o\n"
                + "        LEFT JOIN\n"
                + "    products FOR SYSTEM_TIME AS OF o.proc_time AS p "
                + "    ON o.product_id = p.id\n"
                + "");
            System.out.println(stmtSet.explain(ExplainDetail.JSON_EXECUTION_PLAN));

            stmtSet.execute();

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.toString());
            System.err.println("任务执行失败:" + e.getMessage());
            System.exit(-1);
        }
    }


}
