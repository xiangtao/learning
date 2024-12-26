package io.learning.flink.sql.sink;

import java.util.concurrent.TimeUnit;
import org.apache.flink.types.Row;

public class RequestHBaseSink extends AbstractSink {

    @Override
    protected void doProcess(Row value, Context context) throws Exception {
        requestHBase();
    }

    private void requestHBase() throws InterruptedException {
        TimeUnit.MILLISECONDS.sleep(100);
    }

}
