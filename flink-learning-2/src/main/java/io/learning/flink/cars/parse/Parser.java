package io.learning.flink.cars.parse;

import io.learning.flink.cars.Message;
import io.learning.flink.cars.Row;

public interface Parser {

    void canParse(Message message);
    Row parse(Message message);

}
