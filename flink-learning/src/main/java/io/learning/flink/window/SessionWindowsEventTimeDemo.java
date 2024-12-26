package io.learning.flink.window;

import io.learning.flink.utils.DateUtils;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class SessionWindowsEventTimeDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple3<String, String, Long>> orangeStream = env.addSource(new DataSource())
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator());

        orangeStream.keyBy(0)
            .window(EventTimeSessionWindows.withGap(Time.seconds(10)))
            .process(
                new ProcessWindowFunction<Tuple3<String, String, Long>, Object, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple key, Context context,
                        Iterable<Tuple3<String, String, Long>> elements, Collector<Object> out)
                        throws Exception {
                        System.out.println(
                            "key=" + key.getField(0) + ", " + new Date() + "==" + context.window()
                                .toString() + " , ");
                        out.collect(elements);
                    }
                }).print("out");

        env.execute("SessionWindow Demo");
    }

    private static class DataSource extends
        RichParallelSourceFunction<Tuple3<String, String, Long>> {

        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Tuple3<String, String, Long>> ctx) throws Exception {

            Tuple3<String, String, Long>[] datas = new Tuple3[]{
                new Tuple3("foo", "10:00:04",
                    DateUtils.parse("2021-11-03 10:00:04", DateUtils.PATTERN_YMDHMS_ONE).getTime()),
                new Tuple3("foo", "10:00:10",
                    DateUtils.parse("2021-11-03 10:00:10", DateUtils.PATTERN_YMDHMS_ONE).getTime()),
                /*new Tuple3("foo", "10:00:24",
                    DateUtils.parse("2021-11-03 10:00:24", DateUtils.PATTERN_YMDHMS_ONE).getTime()),

                new Tuple3("foo1", "10:00:04",
                    DateUtils.parse("2021-11-03 10:00:04", DateUtils.PATTERN_YMDHMS_ONE).getTime()),*/
                /*new Tuple3("foo1", "10:00:10",
                    DateUtils.parse("2021-11-03 10:00:10", DateUtils.PATTERN_YMDHMS_ONE).getTime()),
                new Tuple3("foo1", "10:00:24",
                    DateUtils.parse("2021-11-03 10:00:24", DateUtils.PATTERN_YMDHMS_ONE).getTime()),*/

            };

            final int numElements = datas.length;
            int i = 0;

            while (running && i < numElements) {
                Thread.sleep(2000);
                ctx.collect(datas[i]);
                System.out.println("sand data:" + datas[i]);
                i++;
            }
            Thread.sleep(1500000);
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static class BoundedOutOfOrdernessGenerator implements
        AssignerWithPeriodicWatermarks<Tuple3<String, String, Long>> {

        private final long maxOutOfOrderness = 10000;
        private long currentMaxTimestamp;

        @Override
        public long extractTimestamp(Tuple3<String, String, Long> row,
            long previousElementTimestamp) {
            long timestamp = row.f2;
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            System.out.println(Thread.currentThread().getId() + "-" + row + ",time=" + stampToDate(
                row.f2.toString()) + ",watermark=" + stampToDate(
                String.valueOf(currentMaxTimestamp - maxOutOfOrderness)));
            return timestamp;
        }

        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }

        private static String stampToDate(String s) {
            String res;
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            long lt = new Long(s);
            Date date = new Date(lt);
            res = simpleDateFormat.format(date);
            return res;
        }
    }
}
