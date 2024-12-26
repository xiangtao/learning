package io.learning.flink.cars;

import io.learning.flink.kafka.TupleKeyedDeserializationSchema;
import java.util.Properties;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class CarsourceStreamComputing {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.put("bootstrap.servers", "g1-bdp-cdhtest-01.dns.guazi.com:9092,g1-bdp-cdhtest-02.dns.guazi.com:9092,g1-bdp-cdhtest-03.dns.guazi.com:9092");
        //props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "dw-xt-cars-group");
        props.put("auto.offset.reset", "latest"); //value 反序列化

        FlinkKafkaConsumer flinkKafkaConsumer011 = new FlinkKafkaConsumer<Tuple2<String,String>>(
                "test_canal_cars",  //kafka topic
                new TupleKeyedDeserializationSchema(),  // String 序列化
                props);
        DataStreamSource<Tuple2<String,String>> dataStreamSource = env.addSource(flinkKafkaConsumer011).setParallelism(1);

        SingleOutputStreamOperator<Row> oper = dataStreamSource.map(new MapFunction<Tuple2<String,String>, Row>() {
            @Override
            public Row map(Tuple2<String, String> keyValue) throws Exception {
                String tableName = keyValue.getField(0);
                if(tableName==null) {
                    return null;
                }else if(tableName.equalsIgnoreCase("car_source")){
                    String value = keyValue.getField(1);
                    String[] arrs = value.split("\t");

                    if(arrs.length < 2) {
                        return null;
                    }
                    String tn = args[0];
                    String oper = args[1];
                    String[] dataArr = new String[arrs.length-2];
                    System.arraycopy(arrs,2,dataArr,0,arrs.length-2);
                    Message message = Message.builder().setTableName(tn).setOperator(DataOperator.aliseValueOf(oper)).setDataArr(dataArr).build();
                }
                return null;
            }
        });

        dataStreamSource.print(); //把从 kafka 读取到的数据打印在控制台

        env.execute("Flink add data source");
    }

}
