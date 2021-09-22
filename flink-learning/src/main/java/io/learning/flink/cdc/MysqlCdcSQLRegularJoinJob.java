package io.learning.flink.cdc;

import io.learning.flink.utils.CheckpointUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * cdc 2.0 双流join
 */
public class MysqlCdcSQLRegularJoinJob {

    public static void main(String[] args) {
        demo3();
    }

    /**
     * cdc table join
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
                + "   'connector' = 'mysql-cdc',\n"
                + "   'hostname' = 'localhost',\n"
                + "   'port' = '3306',\n"
                + "   'username' = 'root',\n"
                + "   'password' = 'root',\n"
                + "   'database-name' = 'flink_sql',\n"
                + "   'table-name' = 'products',\n"
                + "   'debezium.database.history.instance.name' = 'products'\n"
                + " )";
            tableEnv.executeSql(ddlProduct);

            String ddlCustomers = ""
                + "CREATE TABLE `customers` (\n"
                + "  `id` INT ,\n"
                + "  `first_name` STRING,\n"
                + "  `last_name` STRING,\n"
                + "  `email` STRING,\n"
                + "   PRIMARY KEY (id) NOT ENFORCED\n"
                + " ) WITH (\n"
                + "   'connector' = 'mysql-cdc',\n"
                + "   'hostname' = 'localhost',\n"
                + "   'port' = '3306',\n"
                + "   'username' = 'root',\n"
                + "   'password' = 'root',\n"
                + "   'database-name' = 'flink_sql',\n"
                + "   'table-name' = 'customers',\n"
                + "   'debezium.database.history.instance.name' = 'customers'\n"
                + " )";
            tableEnv.executeSql(ddlCustomers);

            String printSink = ""
                + "CREATE TABLE enriched_orders_print (\n"
                + "   order_number INT,\n"
                + "   order_date TIMESTAMP(0),\n"
                + "   quantity INT,\n"
                + "   product_id INT,\n"
                + "   product_name STRING,\n"
                + "   product_price DECIMAL(10, 2),\n"
                + "   customer_id INT,\n"
                + "   customer_name STRING,\n"
                + "   PRIMARY KEY (order_number) NOT ENFORCED\n"
                + " ) WITH (\n"
                + "     'connector' = 'print'\n"
                + " )";
            tableEnv.executeSql(printSink);

            String jdbcSink = ""
                + "CREATE TABLE enriched_orders_mysql (\n"
                + "   order_number INT,\n"
                + "   order_date TIMESTAMP(0),\n"
                + "   quantity INT,\n"
                + "   product_id INT,\n"
                + "   product_name STRING,\n"
                + "   product_price DECIMAL(10, 2),\n"
                + "   customer_id INT,\n"
                + "   customer_name STRING,\n"
                + "   PRIMARY KEY (order_number) NOT ENFORCED\n"
                + " ) WITH (\n"
                + "    'connector' = 'jdbc',\n"
                + "    'url' = 'jdbc:mysql://localhost:3306/flink_sql',\n"
                + "    'username'='root',\n"
                + "    'password'='root',\n"
                + "    'sink.buffer-flush.max-rows'='0',"
                + "    'table-name' = 'enriched_orders_mysql'"
                + " )";
            tableEnv.executeSql(jdbcSink);


            StatementSet stmtSet = tableEnv.createStatementSet();
            stmtSet.addInsertSql(""
                + "INSERT INTO enriched_orders_mysql\n"
                + " SELECT \n"
                + "    o.order_number,\n"
                + "    o.order_date,\n"
                + "    o.quantity,\n"
                + "    p.id AS product_id,\n"
                + "    p.name AS product_name,\n"
                + "    p.price AS product_price,\n"
                + "    c.id AS customer_id,\n"
                + "    c.first_name AS customer_name\n"
                + "FROM\n"
                + "    orders AS o\n"
                + "        LEFT JOIN\n"
                + "    products AS p ON o.product_id = p.id\n"
                + "        LEFT JOIN\n"
                + "    customers AS c ON o.purchaser = c.id");

            stmtSet.addInsertSql(""
                + "INSERT INTO enriched_orders_print\n"
                + " SELECT \n"
                + "    o.order_number,\n"
                + "    o.order_date,\n"
                + "    o.quantity,\n"
                + "    p.id AS product_id,\n"
                + "    p.name AS product_name,\n"
                + "    p.price AS product_price,\n"
                + "    c.id AS customer_id,\n"
                + "    c.first_name AS customer_name\n"
                + "FROM\n"
                + "    orders AS o\n"
                + "        LEFT JOIN\n"
                + "    products AS p ON o.product_id = p.id\n"
                + "        LEFT JOIN\n"
                + "    customers AS c ON o.purchaser = c.id");
            System.out.println(stmtSet.explain(ExplainDetail.JSON_EXECUTION_PLAN));

//            stmtSet.execute();


        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.toString());
            System.err.println("任务执行失败:" + e.getMessage());
            System.exit(-1);
        }
    }


    /**
     * kafka Append-only stream join cdc table
     */
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

            String ddlOrder = ""
                + "CREATE TABLE orders (\n"
                + " order_number INT,\n"
                + " order_date AS localtimestamp,\n"
                + " purchaser INT,\n"
                + " quantity INT,\n"
                + " product_id INT\n"
                + ") WITH (\n"
                + " 'connector' = 'datagen',\n"
                + " -- optional options --\n"
                + " 'rows-per-second'='1',\n"
                + " 'fields.order_number.kind'='sequence',\n"
                + " 'fields.order_number.start'='10001',\n"
                + " 'fields.order_number.end'='10005',\n"
                + "\n"
                + " 'fields.purchaser.min'='1001',\n"
                + " 'fields.purchaser.max'='1003',\n"
                + "\n"
                + " 'fields.quantity.min'='1',\n"
                + " 'fields.quantity.max'='10',\n"
                + "\n"
                + " 'fields.product_id.min'='102',\n"
                + " 'fields.product_id.max'='107'\n"
                + ")";
            tableEnv.executeSql(ddlOrder);

            // ./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic orders_kafka
            // ./kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic orders_kafka
            // {"order_number":10001,"order_date":"2021-09-01 12:00:00","purchaser":1001,"quantity":1,"product_id":102}
            // {"order_number":10002,"order_date":"2021-09-01 12:00:00","purchaser":1002,"quantity":1,"product_id":102}
            String ddlOrderKafka = ""
                + "CREATE TABLE orders_kafka (\n"
                + " order_number INT,\n"
                + " order_date TIMESTAMP(0),\n"
                + " purchaser INT,\n"
                + " quantity INT,\n"
                + " product_id INT\n"
                + ") WITH (\n"
                + " 'connector' = 'kafka',\n"
                + " 'topic'='orders_kafka',\n"
                + " 'properties.bootstrap.servers'='localhost:9092',\n"
                + " 'properties.group.id'='testGroup',\n"
                + " 'scan.startup.mode'='earliest-offset',\n"
                + " 'format'='json'\n"
                + ")";
            tableEnv.executeSql(ddlOrderKafka);



            String ddlProduct = ""
                + "CREATE TABLE `products` (\n"
                + "  `id` INT,\n"
                + "  `name` STRING,\n"
                + "  `description` STRING,\n"
                + "  `price` decimal(10,2),\n"
                + "   PRIMARY KEY (id) NOT ENFORCED\n"
                + " ) WITH (\n"
                + "   'connector' = 'mysql-cdc',\n"
                + "   'hostname' = 'localhost',\n"
                + "   'port' = '3306',\n"
                + "   'username' = 'root',\n"
                + "   'password' = 'root',\n"
                + "   'database-name' = 'flink_sql',\n"
                + "   'table-name' = 'products',\n"
                + "   'debezium.database.history.instance.name' = 'products'\n"
                + " )";
            tableEnv.executeSql(ddlProduct);

            String ddlCustomers = ""
                + "CREATE TABLE `customers` (\n"
                + "  `id` INT ,\n"
                + "  `first_name` STRING,\n"
                + "  `last_name` STRING,\n"
                + "  `email` STRING,\n"
                + "   PRIMARY KEY (id) NOT ENFORCED\n"
                + " ) WITH (\n"
                + "   'connector' = 'mysql-cdc',\n"
                + "   'hostname' = 'localhost',\n"
                + "   'port' = '3306',\n"
                + "   'username' = 'root',\n"
                + "   'password' = 'root',\n"
                + "   'database-name' = 'flink_sql',\n"
                + "   'table-name' = 'customers',\n"
                + "   'debezium.database.history.instance.name' = 'customers'\n"
                + " )";
            tableEnv.executeSql(ddlCustomers);

            String printSink = ""
                + "CREATE TABLE enriched_orders (\n"
                + "   order_number INT,\n"
                + "   order_date TIMESTAMP(0),\n"
                + "   quantity INT,\n"
                + "   product_id INT,\n"
                + "   product_name STRING,\n"
                + "   product_price DECIMAL(10, 2),\n"
                + "   customer_id INT,\n"
                + "   customer_name STRING\n"
                //+ "   PRIMARY KEY (order_number) NOT ENFORCED\n"
                + " ) WITH (\n"
                + "     'connector' = 'print'\n"
                + " )";
            tableEnv.executeSql(printSink);

            StatementSet stmtSet = tableEnv.createStatementSet();
            stmtSet.addInsertSql(""
                + "INSERT INTO enriched_orders\n"
                + " SELECT \n"
                + "    o.order_number,\n"
                + "    o.order_date,\n"
                + "    o.quantity,\n"
                + "    p.id AS product_id,\n"
                + "    p.name AS product_name,\n"
                + "    p.price AS product_price,\n"
                + "    c.id AS customer_id,\n"
                + "    c.first_name AS customer_name\n"
                + "FROM\n"
                + "    orders_kafka AS o\n"
                + "        LEFT JOIN\n"
                + "    products AS p ON o.product_id = p.id\n"
                + "        LEFT JOIN\n"
                + "    customers AS c ON o.purchaser = c.id");

            stmtSet.execute();

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.toString());
            System.err.println("任务执行失败:" + e.getMessage());
            System.exit(-1);
        }
    }


    /**
     * 对 GMV 进行天级别的全站统计。包含插入/更新/删除，只有付款的订单才能计算进入 GMV ，观察 GMV 值的变化。
     */
    private static void demo3() {
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
                + "   order_status INT,\n"
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
                + "   'connector' = 'mysql-cdc',\n"
                + "   'hostname' = 'localhost',\n"
                + "   'port' = '3306',\n"
                + "   'username' = 'root',\n"
                + "   'password' = 'root',\n"
                + "   'database-name' = 'flink_sql',\n"
                + "   'table-name' = 'products',\n"
                + "   'debezium.database.history.instance.name' = 'products'\n"
                + " )";
            tableEnv.executeSql(ddlProduct);


            String kafkaSink = ""
                + "CREATE TABLE kafka_gmv (\n"
                + "  day_str STRING,\n"
                + "  gmv DECIMAL(10, 5)\n"
                + ") WITH (\n"
                + "    'connector' = 'kafka',\n"
                + "    'topic' = 'kafka_gmv',\n"
                + "    'scan.startup.mode' = 'earliest-offset',\n"
                + "    'properties.group.id' = 'kafka_gmv',\n"
                + "    'properties.bootstrap.servers' = 'localhost:9092',\n"
                + "    'format' = 'debezium-json',\n"
                + "    'debezium-json.ignore-parse-errors' = 'true',\n"
                + "    'debezium-json.timestamp-format.standard' = 'SQL',\n"
                + "    'debezium-json.map-null-key.mode' = 'DROP'"
                + ")";
            tableEnv.executeSql(kafkaSink);


            // -- 读取 Kafka 的 changelog 数据，观察 materialize 后的结果
            String printSink = ""
                + "CREATE TABLE print_table (\n"
                + "                day_str STRING,\n"
                + "                gmv DECIMAL(10, 5)\n"
                + "            ) WITH (\n"
                + "                'connector' = 'print'\n"
                + "            )";
            tableEnv.executeSql(printSink);


            StatementSet stmtSet = tableEnv.createStatementSet();
            stmtSet.addInsertSql(
                  "INSERT INTO kafka_gmv \n"
                + "SELECT DATE_FORMAT(o.order_date, 'yyyy-MM-dd') as day_str, SUM(p.price*o.quantity) as gmv\n"
                + "FROM orders as o \n"
                      + "left join products as p \n"
                      + "on o.product_id=p.id "
                + "WHERE o.order_status = 1\n"
                + "GROUP BY DATE_FORMAT(o.order_date, 'yyyy-MM-dd')");

//            System.out.println(stmtSet.explain(ExplainDetail.JSON_EXECUTION_PLAN));

            stmtSet.addInsertSql(""
                + "insert into print_table SELECT * FROM kafka_gmv"
                + "");
            stmtSet.execute();


        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.toString());
            System.err.println("任务执行失败:" + e.getMessage());
            System.exit(-1);
        }
    }

}
