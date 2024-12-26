package com.dkl.hudi.java.dynamiccols;


import static org.apache.hudi.config.HoodieIndexConfig.BLOOM_INDEX_FILTER_DYNAMIC_MAX_ENTRIES;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.bootstrap.index.NoOpBootstrapIndex;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodiePayloadConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.hive.HiveSyncConfigHolder;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.hive.MultiPartKeysValueExtractor;
import org.apache.hudi.hive.ddl.HiveSyncMode;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.sync.common.HoodieSyncConfig;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class DynamicData2Hudi {

    private static final Logger LOG = LogManager.getLogger(DynamicData2Hudi.class);

    private final String INSERT_OPERATION = WriteOperationType.INSERT.value();
    private final String UPSERT_OPERATION = WriteOperationType.UPSERT.value();
    private final String DELETE_OPERATION = WriteOperationType.DELETE.value();

    protected final String NULL_RECORDKEY_PLACEHOLDER = "__null__";
    protected final String EMPTY_RECORDKEY_PLACEHOLDER = "__empty__";

    protected final String DEFAULT_PARTITION_PATH = "default";
    protected final String DEFAULT_PARTITION_PATH_SEPARATOR = "/";

    private String coreSitePath = "core-site.xml";
    private String hdfsSitePath = "hdfs-site.xml";
    private String hiveSitePath = "hive-site.xml";

    private String tableType = HoodieTableType.COPY_ON_WRITE.name();
    private String dbName = "test_hudi";
    //目标表，hudi表
    private String targetTable = "test_hudi_target_2";
    private String targetTablePath = "/tmp/test_hudi/test_hudi_target_2";

    private String recordKeyFields = null;
    private String orderingField = "ts";
    private String preCombineField = "ts";
    private String partitionFields = "dt";
    private Long smallFileLimit = 25 * 1024 * 1024L;
    private Long maxFileSize = 32 * 1024 * 1024L;
    private Integer recordSizeEstimate = 64;

    private String writeOperationType = INSERT_OPERATION;
    private HoodieJavaWriteClient<HoodieRecordPayload> writeClient = null;

    private boolean shouldCombine = false;
    private boolean shouldOrdering = false;
    private String payloadClassName = "";


    public void init() {
        dbName = "test_hudi";
        //目标表，hudi表
        targetTable = "test_hudi_target_mor";
        targetTablePath = "/tmp/test_hudi/test_hudi_target_mor";

        tableType = HoodieTableType.MERGE_ON_READ.name();

        recordKeyFields = null;
        orderingField = "ts";
        preCombineField = "ts";
        partitionFields = "dt";

        writeOperationType = INSERT_OPERATION;

    }


    public void execute() throws Exception {
        init();

        Schema schema = loadTableSchema();
        Configuration hadoopConf = getHadoopConfig();
        initTable(schema, hadoopConf);
        List<LinkedHashMap<String, Object>> rows = getRows();
        try {
            write2Hudi(schema, hadoopConf, rows);
        } finally {
            closeWriteClient();
        }
        //同步Hive元数据
        HiveConf hiveConf = getHiveConf(hiveSitePath, coreSitePath, hdfsSitePath);
        syncHive(getHiveSyncProperties(targetTablePath), hiveConf);
    }

    protected void closeWriteClient() {
        if (writeClient != null) {
            writeClient.close();
            writeClient = null;
        }
    }

    public Schema loadTableSchema() throws Exception {
        String structName = "hoodie_" + targetTable + "_record";
        String recordNamespace = "hoodie." + targetTable;
        FieldAssembler<Schema> fieldAssembler = SchemaBuilder
            .record(structName)
            .namespace(recordNamespace)
            .fields();
        String[] cola = new String[]{
            "event",
            "platform",
            "module_id",
            "ts",
            "prop1", "prop2", "prop3", "prop4", "prop5",
            "dt"};
        List<String> cols = Arrays.asList(cola);
        for (String key : cols) {
            fieldAssembler = fieldAssembler.optionalString(key);
        }
        return fieldAssembler.endRecord();
    }


    private Configuration getHadoopConfig() {
        Configuration hadoopConf = new Configuration();
        hadoopConf.addResource(
            new Path(this.getClass().getClassLoader().getResource(coreSitePath).getPath()));
        hadoopConf.addResource(
            new Path(this.getClass().getClassLoader().getResource(hdfsSitePath).getPath()));
        return hadoopConf;
    }

    public void initTable(Schema writeSchema, Configuration hadoopConf) throws IOException {
        FileSystem fs = null;
        try {
            fs = FSUtils.getFs(targetTablePath, hadoopConf);

            Path path = new Path(targetTablePath);
            Path hoodiePath = new Path(
                targetTablePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME);

            List<String> writeFiledNames = writeSchema.getFields().stream().map(Schema.Field::name)
                .collect(Collectors.toList());
            shouldCombine =
                writeOperationType.equals(UPSERT_OPERATION) && writeFiledNames.contains(
                    preCombineField);
            shouldOrdering =
                writeOperationType.equals(UPSERT_OPERATION) && writeFiledNames.contains(
                    orderingField);
            payloadClassName = shouldOrdering ? DefaultHoodieRecordPayload.class.getName() :
                shouldCombine ? OverwriteWithLatestAvroPayload.class.getName()
                    : HoodieAvroPayload.class.getName();

            if (!(fs.exists(path) && fs.exists(hoodiePath))) { //根据Hudi路径存不存在，判断Hudi表需不需要初始化
                if (Arrays.asList(INSERT_OPERATION, UPSERT_OPERATION)
                    .contains(writeOperationType)) {
                    HoodieTableMetaClient.withPropertyBuilder()
                        .setTableType(tableType)
                        .setTableName(targetTable)
                        .setPayloadClassName(payloadClassName)
                        .setRecordKeyFields(recordKeyFields)
                        .setPreCombineField(preCombineField)
                        .setPartitionFields(partitionFields)
                        .setBootstrapIndexClass(NoOpBootstrapIndex.class.getName())
                        .initTable(hadoopConf, targetTablePath);
                } else if (writeOperationType.equals(DELETE_OPERATION)) { //Delete操作，Hudi表必须已经存在
                    throw new TableNotFoundException(targetTablePath);
                }
            }
        } finally {
            if (fs != null) {
                fs.close();
            }
        }
    }

    private List<LinkedHashMap<String, Object>> getRows() {
        List<LinkedHashMap<String, Object>> rows = Lists.newArrayList();
        LinkedHashMap<String, Object> row = Maps.newLinkedHashMap();
        row.put("event", "appstart");
        row.put("platform", "dfzx_Android");
        row.put("module_id", "home");
        row.put("ts", "12222");
        row.put("prop1", "1");
        row.put("prop2", "2");
        row.put("prop3", "3");
        row.put("dt", "2023-10-15");
        rows.add(row);

        LinkedHashMap<String, Object> row2 = Maps.newLinkedHashMap();
        row2.put("event", "appstart2");
        row2.put("platform", "dfzx_Android2");
        row2.put("module_id", "home2");
        row2.put("ts", "122222");
        row2.put("prop1", "1");
        row2.put("prop2", "2");
        row2.put("prop3", "3");
        row2.put("dt", "2023-10-15");
        rows.add(row2);

        for (int i = 0; i < 100; i++) {
            LinkedHashMap<String, Object> row_i = Maps.newLinkedHashMap();
            row_i.put("event", "appstart2" + i);
            row_i.put("platform", "dfzx_Android2");
            row_i.put("module_id", "home2");
            row_i.put("ts", "122222" + i);
            row_i.put("prop1", "1");
            row_i.put("prop2", "2");
            row_i.put("prop3", "3");
            row_i.put("dt", "2023-10-15");
            rows.add(row2);
        }
        return rows;
    }


    public void write2Hudi(Schema writeSchema, Configuration hadoopConf,
        List<LinkedHashMap<String, Object>> rows) throws SQLException, IOException {
        Properties indexProperties = new Properties();
        indexProperties.put(BLOOM_INDEX_FILTER_DYNAMIC_MAX_ENTRIES.key(),
            150000); // 1000万总体时间提升1分钟
        HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(targetTablePath)
            .withSchema(writeSchema.toString())
            .withParallelism(2, 2).withDeleteParallelism(2)
            .forTable(targetTable)
            .withWritePayLoad(payloadClassName)
            .withPayloadConfig(
                HoodiePayloadConfig.newBuilder().withPayloadOrderingField(orderingField)
                    .build())
            .withIndexConfig(HoodieIndexConfig.newBuilder()
                .withIndexType(HoodieIndex.IndexType.BLOOM)
//                            .bloomIndexPruneByRanges(false) // 1000万总体时间提升1分钟
                .bloomFilterFPP(0.000001)   // 1000万总体时间提升3分钟
                .fromProperties(indexProperties)
                .build())
            .withCompactionConfig(HoodieCompactionConfig.newBuilder()
                .compactionSmallFileSize(smallFileLimit)
                .approxRecordSize(recordSizeEstimate).build())
            .withArchivalConfig(
                HoodieArchivalConfig.newBuilder().archiveCommitsWith(150, 200).build())
            .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(100).build())
            .combineInput(false, false)
            .withStorageConfig(HoodieStorageConfig.newBuilder()
                .parquetMaxFileSize(maxFileSize).build())
            .build();

        writeClient = new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(hadoopConf), cfg);

        String newCommitTime = writeClient.startCommit();

        // 数据量大时，需要分批写，比如10万一个批次
        List<HoodieRecord<HoodieRecordPayload>> records = generateRecord(rows, writeSchema,
            payloadClassName, shouldCombine);
        if (writeOperationType.equals(UPSERT_OPERATION)) {
            LOG.info("start upsert");
            writeClient.upsert(records, newCommitTime);
        } else {
            LOG.info("start insert");
            writeClient.insert(records, newCommitTime);
        }
        LOG.info("write data finished");
    }

    public void syncHive(TypedProperties properties, HiveConf hiveConf) {
        HiveSyncTool hiveSyncTool = new HiveSyncTool(properties, hiveConf);
        hiveSyncTool.syncHoodieTable();

        LOG.info("sync table to hive finished");
    }

    public HiveConf getHiveConf(String hiveSitePath, String coreSitePath,
        String hdfsSitePath) {
        HiveConf configuration = new HiveConf();
        configuration.addResource(new Path(this.getClass().getClassLoader()
            .getResource(hiveSitePath).getPath()));
        configuration.addResource(new Path(this.getClass().getClassLoader()
            .getResource(coreSitePath).getPath()));
        configuration.addResource(new Path(this.getClass().getClassLoader()
            .getResource(hdfsSitePath).getPath()));
        return configuration;
    }

    /**
     * 同步Hive元数据的一些属性配置
     *
     * @param basePath
     * @return
     */
    public TypedProperties getHiveSyncProperties(String basePath) {
        TypedProperties properties = new TypedProperties();
        properties.put(HiveSyncConfigHolder.METASTORE_URIS.key(),
            "thrift://master-1-1.c-35f92457216d94cd.cn-beijing.emr.aliyuncs.com:9083");
        properties.put(HiveSyncConfigHolder.HIVE_URL.key(), "jdbc:hive2://172.19.17.107:10000");
        properties.put(HiveSyncConfigHolder.HIVE_SYNC_MODE.key(), HiveSyncMode.JDBC.name());
        properties.put(HiveSyncConfigHolder.HIVE_CREATE_MANAGED_TABLE.key(), true);
        properties.put(HoodieSyncConfig.META_SYNC_DATABASE_NAME.key(), dbName);
        properties.put(HoodieSyncConfig.META_SYNC_TABLE_NAME.key(), targetTable);
        properties.put(HoodieSyncConfig.META_SYNC_BASE_PATH.key(), basePath);
        properties.put(HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS.key(),
            MultiPartKeysValueExtractor.class.getName());
        properties.put(HoodieSyncConfig.META_SYNC_PARTITION_FIELDS.key(), partitionFields);
        if (partitionFields != null && !partitionFields.isEmpty()) {
            properties.put(HoodieSyncConfig.META_SYNC_PARTITION_FIELDS.key(), partitionFields);
        }

        return properties;
    }


    public List<HoodieRecord<HoodieRecordPayload>> generateRecord(
        List<LinkedHashMap<String, Object>> rows,
        Schema writeSchema,
        String payloadClassName,
        boolean shouldCombine) throws IOException, SQLException {
        List<HoodieRecord<HoodieRecordPayload>> list = new ArrayList<>();

        for (LinkedHashMap<String, Object> row : rows) {
            GenericRecord rec = new GenericData.Record(writeSchema);
            writeSchema.getFields().forEach(field -> {
                try {
                    rec.put(field.name(),
                        convertValueType(row, field.name(), field.schema().getType()));
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });

            String partitionPath =
                partitionFields == null ? "" : getRecordPartitionPath(row, writeSchema);
            System.out.println(partitionPath);
            String rowKey = recordKeyFields == null
                ? UUID.randomUUID().toString() : getRecordKey(row, writeSchema);
            HoodieKey key = new HoodieKey(rowKey, partitionPath);
            if (shouldCombine) {
                Object orderingVal = HoodieAvroUtils.getNestedFieldVal(rec, preCombineField, false,
                    false);
                list.add(new HoodieAvroRecord<>(key,
                    createPayload(payloadClassName, rec, (Comparable) orderingVal)));
            } else {
                list.add(new HoodieAvroRecord<>(key, createPayload(payloadClassName, rec)));
            }
        }
        return list;
    }

    /**
     * Create a payload class via reflection, do not ordering/preCombine value.
     */
    public HoodieRecordPayload createPayload(String payloadClass, GenericRecord record)
        throws IOException {
        try {
            return (HoodieRecordPayload) ReflectionUtils.loadClass(payloadClass,
                new Class<?>[]{Option.class}, Option.of(record));
        } catch (Throwable e) {
            throw new IOException("Could not create payload for class: " + payloadClass, e);
        }
    }

    /**
     * Create a payload class via reflection, passing in an ordering/preCombine value.
     */
    public HoodieRecordPayload createPayload(String payloadClass, GenericRecord record,
        Comparable orderingVal)
        throws IOException {
        try {
            return (HoodieRecordPayload) ReflectionUtils.loadClass(payloadClass,
                new Class<?>[]{GenericRecord.class, Comparable.class}, record, orderingVal);
        } catch (Throwable e) {
            throw new IOException("Could not create payload for class: " + payloadClass, e);
        }
    }

    /**
     * 从ResultSet中获取主键字段对应的值
     *
     * @param row
     * @param writeSchema
     * @return
     * @throws SQLException
     */
    private String getRecordKey(LinkedHashMap<String, Object> row, Schema writeSchema)
        throws SQLException {
        boolean keyIsNullEmpty = true;
        StringBuilder recordKey = new StringBuilder();
        for (String recordKeyField : recordKeyFields.split(",")) {
            String recordKeyValue = getNestedFieldValAsString(row, writeSchema, recordKeyField);
            recordKeyField = recordKeyField.toLowerCase();
            if (recordKeyValue == null) {
                recordKey.append(recordKeyField + ":" + NULL_RECORDKEY_PLACEHOLDER + ",");
            } else if (recordKeyValue.isEmpty()) {
                recordKey.append(recordKeyField + ":" + EMPTY_RECORDKEY_PLACEHOLDER + ",");
            } else {
                recordKey.append(recordKeyField + ":" + recordKeyValue + ",");
                keyIsNullEmpty = false;
            }
        }
        recordKey.deleteCharAt(recordKey.length() - 1);
        if (keyIsNullEmpty) {
            throw new HoodieKeyException("recordKey values: \"" + recordKey + "\" for fields: "
                + recordKeyFields.toString() + " cannot be entirely null or empty.");
        }
        return recordKey.toString();
    }

    /**
     * 从ResultSet中获取主键字段对应的值
     *
     * @param row
     * @param writeSchema
     * @return
     * @throws SQLException
     */
    private String getRecordPartitionPath(LinkedHashMap<String, Object> row,
        Schema writeSchema)
        throws SQLException {
        if (partitionFields.isEmpty()) {
            return "";
        }

        StringBuilder partitionPath = new StringBuilder();
        String[] avroPartitionPathFields = partitionFields.split(",");
        for (String partitionPathField : avroPartitionPathFields) {
            String fieldVal = getNestedFieldValAsString(row, writeSchema, partitionPathField);
            if (fieldVal == null || fieldVal.isEmpty()) {
                partitionPath.append(partitionPathField + "=" + DEFAULT_PARTITION_PATH);
            } else {
                partitionPath.append(partitionPathField + "=" + fieldVal);
            }
            partitionPath.append(DEFAULT_PARTITION_PATH_SEPARATOR);
        }
        partitionPath.deleteCharAt(partitionPath.length() - 1);
        return partitionPath.toString();
    }

    /**
     * 根据字段名，从ResultSet获取对应的值
     *
     * @param row
     * @param writeSchema
     * @param fieldName
     * @return
     * @throws SQLException
     */
    private String getNestedFieldValAsString(LinkedHashMap<String, Object> row,
        Schema writeSchema,
        String fieldName) throws SQLException {
        Object value = null;
        if (writeSchema.getFields().stream().map(field -> field.name()).collect(Collectors.toList())
            .contains(fieldName)) {
            value = convertValueType(row, fieldName,
                writeSchema.getField(fieldName).schema().getType());
        }
        return StringUtils.objToString(value);
    }

    /**
     * 根据字段名和字段数据类型，反正ResultSet中对应的字段的正确数据类型的值
     *
     * @param row
     * @param name
     * @param dataType
     * @return
     * @throws SQLException
     */
    protected Object convertValueType(LinkedHashMap<String, Object> row, String name,
        Schema.Type dataType)
        throws SQLException {
        Object value = null;
        if (dataType != null) {
            switch (dataType.toString().toLowerCase()) {
                case "int":
                case "smallint":
                case "tinyint":
                    value = row.get(name);
                    break;
                case "bigint":
                case "long":
                    value = row.get(name);
                    break;
                case "float":
                    value = row.get(name);
                    break;
                case "double":
                    value = row.get(name);
                    break;
                case "string":
                    value = row.get(name);
                    break;
                default:
                    if (row.get(name) == null) {
                        value = null;
                    } else {
                        value = row.get(name);
                    }
            }
        }
        return value;
    }

    public static void main(String[] args) throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                DynamicData2Hudi dynamicData2Hudi = new DynamicData2Hudi();
                try {
                    dynamicData2Hudi.execute();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            ;
        });

        executorService.submit(new Runnable() {
            @Override
            public void run() {
                DynamicData2Hudi dynamicData2Hudi = new DynamicData2Hudi();
                try {
                    dynamicData2Hudi.execute();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            ;
        });

        executorService.submit(new Runnable() {
            @Override
            public void run() {
                DynamicData2Hudi dynamicData2Hudi = new DynamicData2Hudi();
                try {
                    dynamicData2Hudi.execute();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            ;
        });

        executorService.awaitTermination(60 * 2, TimeUnit.SECONDS);

    }
}
