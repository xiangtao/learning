/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dkl.hudi.java;

import java.net.URL;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.*;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.HoodieIndex.IndexType;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


/**
 * Simple examples of #{@link HoodieJavaWriteClient}.
 * <p>
 * Usage: HoodieJavaWriteClientExample <tablePath> <tableName>
 * <tablePath> and <tableName> describe root path of hudi and table name
 * for example, `HoodieJavaWriteClientExample file:///tmp/hoodie/sample-table hoodie_rt`
 */
public class HoodieJavaWriteClientExample {

    private static final Logger LOG = LogManager.getLogger(HoodieJavaWriteClientExample.class);

    private static String tableType = HoodieTableType.COPY_ON_WRITE.name();

    private static String coreSitePath = "core-site.xml";
    private static String hdfsSitePath = "hdfs-site.xml";

    public static void main(String[] args) throws Exception {
        /*if (args.length < 2) {
            System.err.println("Usage: HoodieJavaWriteClientExample <tablePath> <tableName>");
            System.exit(1);
        }
        String tablePath = args[0];
        String tableName = args[1];*/

        //目标表，hudi表
        String tablePath = "/input/hudi/test_hudi_target_cow";
        String tableName = "test_hudi_target_cow";

        // Generator of some records to be loaded in.
        HoodieExampleDataGenerator<HoodieAvroPayload> dataGen = new HoodieExampleDataGenerator<>();

        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        hadoopConf.set("fs.hdfs.impl.disable.cache", "true");
        URL corePath = HoodieJavaWriteClientExample.class.getClassLoader()
            .getResource(coreSitePath);
        URL hdfsPath = HoodieJavaWriteClientExample.class.getClassLoader()
            .getResource(hdfsSitePath);
        hadoopConf.addResource(corePath);
        hadoopConf.addResource(hdfsPath);

        // initialize the table, if not done already
        Path path = new Path(tablePath);
        FileSystem fs = FSUtils.getFs(tablePath, hadoopConf);
        if (!fs.exists(path)) {
            HoodieTableMetaClient.withPropertyBuilder()
                .setTableType(tableType)
                .setTableName(tableName)
                .setPayloadClassName(HoodieAvroPayload.class.getName())
                .initTable(hadoopConf, tablePath);
        }

        // Create the write client to write some records in
        HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(tablePath)
            .withSchema(HoodieExampleDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
            .withDeleteParallelism(2).forTable(tableName)
            .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(IndexType.INMEMORY).build())
            .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(3, 5).build())
            .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(2).build())
            .build();
        HoodieJavaWriteClient<HoodieAvroPayload> client =
            new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(hadoopConf), cfg);

        // inserts
        /*String newCommitTime = client.startCommit();
        LOG.info("Starting commit " + newCommitTime);

        List<HoodieRecord<HoodieAvroPayload>> records = dataGen.generateInserts2(newCommitTime, 2);
        List<HoodieRecord<HoodieAvroPayload>> recordsSoFar = new ArrayList<>(records);
        List<HoodieRecord<HoodieAvroPayload>> writeRecords =
            recordsSoFar.stream().map(r -> new HoodieAvroRecord<HoodieAvroPayload>(r)).collect(Collectors.toList());
        client.insert(writeRecords, newCommitTime);*/

        // updates
        String newCommitTime = client.startCommit();
        LOG.info("Starting commit " + newCommitTime);
        List<HoodieRecord<HoodieAvroPayload>> toBeUpdated = dataGen.generateInserts2(newCommitTime, 3);
        //records.addAll(toBeUpdated);
        List<HoodieRecord<HoodieAvroPayload>> recordsSoFar = new ArrayList<>();
        recordsSoFar.addAll(toBeUpdated);
        List<HoodieRecord<HoodieAvroPayload>> writeRecords =
            recordsSoFar.stream().map(r -> new HoodieAvroRecord<HoodieAvroPayload>(r)).collect(Collectors.toList());
        client.upsert(writeRecords, newCommitTime);

        // Delete
        /*newCommitTime = client.startCommit();
        LOG.info("Starting commit " + newCommitTime);
        // just delete half of the records
        int numToDelete = recordsSoFar.size() / 2;
        List<HoodieKey> toBeDeleted =
            recordsSoFar.stream().map(HoodieRecord::getKey).limit(numToDelete).collect(Collectors.toList());
        client.delete(toBeDeleted, newCommitTime);*/

        client.close();

    }
}
