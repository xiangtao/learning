package io.learning.flink.kafka;

import java.io.IOException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

/**
 *
 */
public class TupleKeyedDeserializationSchema implements KeyedDeserializationSchema {
    @Override
    public Tuple2<String, String> deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) throws IOException {

        Tuple2<String, String> data = new Tuple2<String, String>();
        if (messageKey != null) {
            data.setField(new String(messageKey),0);
        }

        if (message != null) {
            data.setField(new String(message),1);
        }
        return data;
    }

    @Override
    public boolean isEndOfStream(Object o) {
        return false;
    }

    @Override
    public TypeInformation<Tuple2<String, String>> getProducedType() {
        return TypeExtractor.getForObject(new Tuple2<String,String>("",""));
    }
}
