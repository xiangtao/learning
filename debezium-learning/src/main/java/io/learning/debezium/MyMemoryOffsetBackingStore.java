package io.learning.debezium;

import io.debezium.embedded.EmbeddedEngine;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.kafka.connect.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyMemoryOffsetBackingStore extends MemoryOffsetBackingStore {
    private static final Logger LOG = LoggerFactory.getLogger(MyMemoryOffsetBackingStore.class);

    public static final String OFFSET_STATE_VALUE = "offset.storage.flink.state.value";
    public static final int FLUSH_TIMEOUT_SECONDS = 10;

    protected Map<ByteBuffer, ByteBuffer> data = new HashMap<>();
    protected ExecutorService executor;

    @Override
    public void configure(WorkerConfig config) {
        // eagerly initialize the executor, because OffsetStorageWriter will use it later
        start();

        Map<String, ?> conf = config.originals();

        DebeziumOffset specificOffset = new DebeziumOffset();
        Map<String, String> sourcePartition = new HashMap<>();
        sourcePartition.put("server", "mysql_binlog_source");
        specificOffset.setSourcePartition(sourcePartition);

        Map<String, Object> sourceOffset = new HashMap<>();
        sourceOffset.put("file", "mysql-bin.000023");
        sourceOffset.put("pos", 15803);
        sourceOffset.put("gtids","a5086c24-90f4-11ea-b66a-4fe6963a633f:3206");
        specificOffset.setSourceOffset(sourceOffset);


        String engineName = (String) conf.get(EmbeddedEngine.ENGINE_NAME.name());
        Converter keyConverter = new JsonConverter();
        Converter valueConverter = new JsonConverter();
        keyConverter.configure(config.originals(), true);
        Map<String, Object> valueConfigs = new HashMap<>(conf);
        valueConfigs.put("schemas.enable", false);
        valueConverter.configure(valueConfigs, true);
        OffsetStorageWriter offsetWriter =
            new OffsetStorageWriter(
                this,
                // must use engineName as namespace to align with Debezium Engine
                // implementation
                engineName,
                keyConverter,
                valueConverter);

        offsetWriter.offset(specificOffset.sourcePartition, specificOffset.sourceOffset);

        // flush immediately
        if (!offsetWriter.beginFlush()) {
            // if nothing is needed to be flushed, there must be something wrong with the
            // initialization
            LOG.warn(
                "Initialize MyMemoryOffsetBackingStore from empty offset state, this shouldn't happen.");
            return;
        }

        // trigger flushing
        Future<Void> flushFuture =
            offsetWriter.doFlush(
                (error, result) -> {
                    if (error != null) {
                        LOG.error("Failed to flush initial offset.", error);
                    } else {
                        LOG.debug("Successfully flush initial offset.");
                    }
                });

        // wait until flushing finished
        try {
            flushFuture.get(FLUSH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            LOG.info(
                "Flush offsets successfully, partition: {}, offsets: {}",
                specificOffset.sourcePartition,
                specificOffset.sourceOffset);
        } catch (InterruptedException e) {
            LOG.warn("Flush offsets interrupted, cancelling.", e);
            offsetWriter.cancelFlush();
        } catch (ExecutionException e) {
            LOG.error("Flush offsets threw an unexpected exception.", e);
            offsetWriter.cancelFlush();
        } catch (TimeoutException e) {
            LOG.error("Timed out waiting to flush offsets to storage.", e);
            offsetWriter.cancelFlush();
        }
    }

    @Override
    public void start() {
        if (executor == null) {
            executor =
                Executors.newFixedThreadPool(
                    1,
                    ThreadUtils.createThreadFactory(
                        this.getClass().getSimpleName() + "-%d", false));
        }
    }

    @Override
    public void stop() {
        if (executor != null) {
            executor.shutdown();
            // Best effort wait for any get() and set() tasks (and caller's callbacks) to complete.
            try {
                executor.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (!executor.shutdownNow().isEmpty()) {
                throw new ConnectException(
                    "Failed to stop FlinkOffsetBackingStore. Exiting without cleanly "
                        + "shutting down pending tasks and/or callbacks.");
            }
            executor = null;
        }
    }

    public Future<Map<ByteBuffer, ByteBuffer>> get(final Collection<ByteBuffer> keys) {
        return executor.submit(
            () -> {
                Map<ByteBuffer, ByteBuffer> result = new HashMap<>();
                for (ByteBuffer key : keys) {
                    result.put(key, data.get(key));
                }
                return result;
            });
    }

    @Override
    public Future<Void> set(
        final Map<ByteBuffer, ByteBuffer> values, final Callback<Void> callback) {
        return executor.submit(
            () -> {
                for (Map.Entry<ByteBuffer, ByteBuffer> entry : values.entrySet()) {
                    data.put(entry.getKey(), entry.getValue());
                }
                if (callback != null) {
                    callback.onCompletion(null, null);
                }
                return null;
            });
    }


}
