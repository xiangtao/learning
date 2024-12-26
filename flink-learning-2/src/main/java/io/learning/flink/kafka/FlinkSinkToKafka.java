package io.learning.flink.kafka;

import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class FlinkSinkToKafka {

    private static final String READ_TOPIC = "student";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "student-group");
        props.put("auto.offset.reset", "latest");

        DataStreamSource<String> student = env.addSource(new FlinkKafkaConsumer<String>(
            READ_TOPIC,
            new SimpleStringSchema(),
            props)).setParallelism(1);
        student.print();

        student.addSink(new FlinkKafkaProducer<>(
            "localhost:9092",
            "student-write",
            new SimpleStringSchema()
        )).name("flink-sink-kafka")
            .setParallelism(5);

        env.execute("flink learning connectors kafka");
    }
}
