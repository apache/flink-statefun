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
package org.apache.flink.statefun.flink.io.kafka;

import java.util.Properties;
import org.apache.flink.statefun.flink.io.common.ReflectionUtil;
import org.apache.flink.statefun.flink.io.spi.SourceProvider;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressDeserializer;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressSpec;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;

public class KafkaSourceProvider implements SourceProvider {

  @Override
  public <T> SourceFunction<T> forSpec(IngressSpec<T> ingressSpec) {
    KafkaIngressSpec<T> spec = asKafkaSpec(ingressSpec);

    Properties properties = new Properties();
    properties.putAll(spec.properties());
    properties.put("bootstrap.servers", spec.kafkaAddress());

    return new FlinkKafkaConsumer<>(spec.topics(), deserializationSchemaFromSpec(spec), properties);
  }

  private static <T> KafkaIngressSpec<T> asKafkaSpec(IngressSpec<T> ingressSpec) {
    if (ingressSpec instanceof KafkaIngressSpec) {
      return (KafkaIngressSpec<T>) ingressSpec;
    }
    if (ingressSpec == null) {
      throw new NullPointerException("Unable to translate a NULL spec");
    }
    throw new IllegalArgumentException(String.format("Wrong type %s", ingressSpec.type()));
  }

  private <T> KafkaDeserializationSchema<T> deserializationSchemaFromSpec(
      KafkaIngressSpec<T> spec) {
    KafkaIngressDeserializer<T> ingressDeserializer =
        ReflectionUtil.instantiate(spec.deserializerClass());
    return new KafkaDeserializationSchemaDelegate<>(ingressDeserializer);
  }
}
