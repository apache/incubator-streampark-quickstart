/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.streampark.flink.quickstart.datastream;

import org.apache.streampark.common.util.JsonUtils;
import org.apache.streampark.flink.connector.function.TransformFunction;
import org.apache.streampark.flink.connector.jdbc.sink.JdbcJavaSink;
import org.apache.streampark.flink.connector.kafka.source.KafkaJavaSource;
import org.apache.streampark.flink.connector.kafka.bean.KafkaRecord;
import org.apache.streampark.flink.core.StreamEnvConfig;
import org.apache.streampark.flink.core.scala.StreamingContext;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * @author benjobs
 */
public class QuickStartJavaApp {

    public static void main(String[] args) {

        StreamEnvConfig envConfig = new StreamEnvConfig(args, null);

        StreamingContext context = new StreamingContext(envConfig);

        DataStream<JavaUser> source = new KafkaJavaSource<String>(context)
                .getDataStream()
                .map((MapFunction<KafkaRecord<String>, JavaUser>) value ->
                        JsonUtils.read(value.value(), JavaUser.class))
                .filter((FilterFunction<JavaUser>) value -> value.age < 30);


        new JdbcJavaSink<JavaUser>(context)
                .sql((TransformFunction<JavaUser, String>) JavaUser::toSql)
                .sink(source);

        context.start();
    }

}

