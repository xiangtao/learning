package io.learning.flink;

import java.util.List;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class SocketTextStreamWordCount {

  public SocketTextStreamWordCount() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("USAGE:\nSocketTextStreamWordCount <hostname> <port>");
    } else {
      String hostname = args[0];
      Integer port = Integer.parseInt(args[1]);
      StreamExecutionEnvironment env =  StreamExecutionEnvironment.getExecutionEnvironment();
      List<String> sources = Lists.newArrayList();
      sources.add("hello 1222");
      sources.add("hello");
      sources.add("hello");
      sources.add("word");
      sources.add("bye");
      sources.add("good");
      DataStreamSource<String> stream = env.fromCollection(sources);
      SingleOutputStreamOperator<Tuple2<String, Integer>> sum = stream
          .flatMap(new SocketTextStreamWordCount.LineSplitter())
          .keyBy(new int[]{0})
          .sum(1);
      sum.print();
      env.execute("Java WordCount from SocketTextStream Example");
    }
  }

  public static final class LineSplitter implements
      FlatMapFunction<String, Tuple2<String, Integer>> {

    public LineSplitter() {
    }

    @Override
    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) {
      String[] tokens = s.toLowerCase().split("\\W+");
      String[] var4 = tokens;
      int length = tokens.length;

      for (int var6 = 0; var6 < length; ++var6) {
        String token = var4[var6];
        if (token.length() > 0) {
          collector.collect(new Tuple2(token, Integer.valueOf(1)));
        }
      }
    }
  }
}
