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

import static org.apache.flink.statefun.flink.io.testutils.YamlUtils.loadAsJsonFromClassResource;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

import com.google.protobuf.Message;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.statefun.flink.io.spi.JsonIngressSpec;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.junit.Test;

public class ProtobufKafkaSourceProviderTest {

  @Test
  public void exampleUsage() {
    JsonNode ingressDefinition =
        loadAsJsonFromClassResource(getClass().getClassLoader(), "protobuf-kafka-ingress.yaml");
    JsonIngressSpec<?> spec =
        new JsonIngressSpec<>(
            ProtobufKafkaIngressTypes.PROTOBUF_KAFKA_INGRESS_TYPE,
            new IngressIdentifier<>(Message.class, "foo", "bar"),
            ingressDefinition);

    ProtobufKafkaSourceProvider provider = new ProtobufKafkaSourceProvider();
    SourceFunction<?> source = provider.forSpec(spec);

    assertThat(source, instanceOf(FlinkKafkaConsumer.class));
  }
}
