package io.learning.kafka;

import com.koolearn.framework.logger.kafka.KafkaProducer;
import io.learning.kafka.kafka.KafkaClient;
import io.learning.kafka.kafka.KafkaProperty;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaClientDemo {

    public static void main(String[] args) throws InterruptedException {
        produce(args);
    }

    public static void consumer() throws InterruptedException {
        KafkaProperty property = new KafkaProperty();
        property.setBootstrapServer("kafka101-1.cluster.koolearn.com:9093");
        property.setTopic("mobile-topic-online");
        property.setGroupId("xt_group_1");
        property.setAutoOffsetReset("earliest"); // earliest

        KafkaClient kafkaClient = KafkaClient.getInstance(property);
        KafkaConsumer consumer = kafkaClient.getKafkaConsumer();

        consumer.subscribe(Arrays.asList(property.getTopic()));

        while (true) {
            System.out.println("ddddd");
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                String str = "offset=%s,timestamp=%s,date=%s,key=%s,value=%s";
                Date date = new Date(record.timestamp());
                System.out.println(
                    String.format(str, record.offset(), record.timestamp(), date, record.key(), record.value()));
            }
            Thread.sleep(1000);
        }
    }

    public static void produce(String[] args) throws InterruptedException {
        String filePath = args[0];
        Integer size = Integer.parseInt(args[1]);

        System.out.println("file=" + filePath);
        System.out.println("size=" + size);

        String zkAddr = "zk1-monitor.cluster.koolearn.com:22181";
        String topic = "mobile-topic-online";
        com.koolearn.framework.logger.kafka.KafkaProducer producer = new KafkaProducer(zkAddr);
        try {
            File file = new File(filePath);
            List<String> list = FileUtils.readLines(file);
            int cnt = 1;
            for (String line : list) {
                if (cnt <= size) {
                    System.out.println(line);
                    producer.send(topic, line);
                } else {
                    break;
                }
                cnt++;
            }
            System.out.println("send size=" + cnt);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

}
