package io.learning.flink.cdc;

import io.learning.flink.utils.JsonUtils;
import java.util.Date;
import lombok.Builder;
import lombok.Data;

public class DataGen {

    // ./kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic flink_sql_test

    public static void main(String[] args) {

        Orders orders = Orders.builder().order_number(10001)
            .order_date("2021-09-01 12:00:00")
            .product_id(102)
            .purchaser(1001)
            .quantity(1)
            .build();
        System.out.println(JsonUtils.toJson(orders));
    }

    @Data
    @Builder
    public static class Orders {

        private int order_number;
        private String order_date;
        private int purchaser;
        private int quantity;
        private int product_id;
    }
}
