package io.learning.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamingJob {
    public StreamingJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.execute("Flink Streaming Java API Skeleton");
    }
}
