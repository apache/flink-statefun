/*
 * Copyright 2019 Ververica GmbH.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.statefun.flink.io.kafka;

import com.google.protobuf.Message;
import com.ververica.statefun.flink.io.spi.JsonIngressSpec;
import com.ververica.statefun.sdk.io.IngressIdentifier;
import com.ververica.statefun.sdk.kafka.Constants;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

public class ProtobufKafkaSourceProviderTest {

  @Test
  public void exampleUsage() {
    JsonNode ingressDefinition = fromPath("protobuf-kafka-ingress.yaml");
    JsonIngressSpec<?> spec =
        new JsonIngressSpec<>(
            Constants.PROTOBUF_KAFKA_INGRESS_TYPE,
            new IngressIdentifier<>(Message.class, "foo", "bar"),
            ingressDefinition);

    ProtobufKafkaSourceProvider provider = new ProtobufKafkaSourceProvider();
    SourceFunction<?> source = provider.forSpec(spec);

    assertThat(source, instanceOf(FlinkKafkaConsumer.class));
  }

  private static JsonNode fromPath(String path) {
    URL moduleUrl = ProtobufKafkaSourceProvider.class.getClassLoader().getResource(path);
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    try {
      return mapper.readTree(moduleUrl);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
