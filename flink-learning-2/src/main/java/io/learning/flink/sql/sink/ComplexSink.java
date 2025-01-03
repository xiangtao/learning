package io.learning.flink.sql.sink;

import io.learning.flink.utils.HashUtil;
import org.apache.flink.types.Row;

public class ComplexSink extends AbstractSink {

    @Override
    protected void doProcess(Row value, Context context) throws Exception {
        String s = value.toString();
        for (int i = 0; i < 1000; i++) {
            s = HashUtil.md5(s);
        }
    }

}
