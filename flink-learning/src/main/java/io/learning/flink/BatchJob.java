/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.learning.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Skeleton for a Flink Batch Job.
 *
 * <p>For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your appliation into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
public class BatchJob {

	public static void main(String[] args) throws Exception {
		// set up the batch execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataSet<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * http://flink.apache.org/docs/latest/apis/batch/index.html
		 *
		 * and the examples
		 *
		 * http://flink.apache.org/docs/latest/apis/batch/examples.html
		 *
		 */

		// execute program
		env.execute("Flink Batch Java API Skeleton");
	}

	public static class SocketTextStreamWordCount {
        public static void main(String[] args) throws Exception {
            //参数检查
            if (args.length != 2) {
                System.err.println("USAGE:\nSocketTextStreamWordCount <hostname> <port>");
                return;
            }

            String hostname = args[0];
            Integer port = Integer.parseInt(args[1]);
            // set up the streaming execution environment
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            //获取数据
            DataStreamSource<String> stream = env.socketTextStream(hostname, port);

            //计数
            SingleOutputStreamOperator<Tuple2<String, Integer>> sum = stream.flatMap(new LineSplitter())
                    .keyBy(0)
                    .sum(1);

            sum.print();

            env.execute("Java WordCount from SocketTextStream Example");
        }

        public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) {
                String[] tokens = s.toLowerCase().split("\\W+");

                for (String token : tokens) {
                    if (token.length() > 0) {
                        collector.collect(new Tuple2<String, Integer>(token, 1));
                    }
                }
            }
        }
    }

	/**
     * Skeleton for a Flink Streaming Job.
     *
     * <p>For a tutorial how to write a Flink streaming application, check the
     * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
     *
     * <p>To package your appliation into a JAR file for execution, run
     * 'mvn clean package' on the command line.
     *
     * <p>If you change the name of the main class (with the public static void main(String[] args))
     * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
     */
    public static class StreamingJob {

        public static void main(String[] args) throws Exception {
            // set up the streaming execution environment
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            /*
             * Here, you can start creating your execution plan for Flink.
             *
             * Start with getting some data from the environment, like
             * 	env.readTextFile(textPath);
             *
             * then, transform the resulting DataStream<String> using operations
             * like
             * 	.filter()
             * 	.flatMap()
             * 	.join()
             * 	.coGroup()
             *
             * and many more.
             * Have a look at the programming guide for the Java API:
             *
             * http://flink.apache.org/docs/latest/apis/streaming/index.html
             *
             */

            // execute program
            env.execute("Flink Streaming Java API Skeleton");
        }
    }
}
