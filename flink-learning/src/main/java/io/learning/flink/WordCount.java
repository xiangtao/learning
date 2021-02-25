package io.learning.flink;

import java.util.Iterator;
import java.util.List;
import org.apache.flink.api.java.functions.FlatMapIterator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCount {


  public static void main(String[] args) throws Exception {

    // Get run environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    List<String> sources = Lists.newArrayList();
    sources.add("a1 b1 a1 b3");
    sources.add("a2 b1 a1 b7");
    sources.add("a3 b3 a4 b5");
    // Add source
    DataStreamSource<String> out = env.fromCollection(sources);

    // transformation
    DataStream dataStream = out.flatMap(new FlatMapIterator<String, Tuple2<String, Integer>>() {
      @Override
      public Iterator<Tuple2<String, Integer>> flatMap(final String s) throws Exception {
        String[] words = s.toLowerCase().split("\\W+");
        List<Tuple2<String, Integer>> out = Lists.newArrayList();
        for (String word : words) {
          out.add(new Tuple2(word, 1));
        }
        return out.iterator();
      }
    }).setParallelism(3).keyBy(0).sum(1);

    // sink
    dataStream.print();

    // submit to execute
    env.execute("WordCount Example");

  }

}
