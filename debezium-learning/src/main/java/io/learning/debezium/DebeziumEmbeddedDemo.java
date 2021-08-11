package io.learning.debezium;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.exception.ExceptionUtils;

public class DebeziumEmbeddedDemo {

  public static void main(String[] args) {
    // Define the configuration for the Debezium Engine with MySQL connector...
    final Properties props = new Properties();
    props.setProperty("name", "my-engine");
    props.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector");
    props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
    props.setProperty("offset.storage.file.filename",
        "/Users/taox/xdf/git/myself/learning/debezium-learning/src/main/resources/offsets.dat");
    props.setProperty("offset.flush.interval.ms", "20000");
    /* begin connector properties */
    props.setProperty("database.hostname", "localhost");
    props.setProperty("database.port", "3306");
    props.setProperty("database.user", "root");
    props.setProperty("database.password", "root");
    props.setProperty("database.server.id", "857442");
    props.setProperty("database.server.name", "my-app-connector2");
    props.setProperty("snapshot.mode", "schema_only");
    props.setProperty("database.include.list", "flink_sql");
    props.setProperty("table.include.list", "flink_sql.orders");
    props.setProperty("database.history",
        "io.debezium.relational.history.FileDatabaseHistory");
    props.setProperty("database.history.file.filename",
        "/Users/taox/xdf/git/myself/learning/debezium-learning/src/main/resources/dbhistory.dat");

    // Create the engine with this configuration ...
    try {
      DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
          .using(props)
          .notifying(record -> {
            System.out.println("key: " + record.key());
            System.out.println("value: " + record.value());
          }).using((success, message, error) -> {
            System.out.println("complete:" + message);
            if (error != null) {
              ExceptionUtils.rethrow(error);
            }
          }).build();

      // Run the engine asynchronously ...
      ExecutorService executor = Executors.newSingleThreadExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
          Thread thread = new Thread(r);
          thread.setDaemon(false);
          return thread;
        }
      });
      executor.execute(engine);
      // Do something else or wait for a signal or an event
      // on a clean exit, wait for the runner thread
      boolean running = true;
      try {
        while (running) {
          if (executor.awaitTermination(5, TimeUnit.SECONDS)) {
            System.out.println("termination");
            break;
          }
          System.out.println("await to termination");
          /*if (error != null) {
            running = false;
            shutdownEngine();
            // rethrow the error from Debezium consumer
            ExceptionUtils.rethrow(error);
          }*/
        }
      } catch (InterruptedException e) {
        // may be the result of a wake-up interruption after an exception.
        // we ignore this here and only restore the interruption state
        Thread.currentThread().interrupt();
      }

      System.out.println("end");
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
    }
    // Engine is stopped when the main code is finished
  }
}
