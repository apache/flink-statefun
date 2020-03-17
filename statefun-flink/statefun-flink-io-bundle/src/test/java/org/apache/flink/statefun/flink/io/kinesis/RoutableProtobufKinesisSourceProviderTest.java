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

package org.apache.flink.statefun.flink.io.kinesis;

import static org.apache.flink.statefun.flink.io.testutils.YamlUtils.loadAsJsonFromClassResource;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.protobuf.Message;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.statefun.flink.io.kinesis.polyglot.RoutableProtobufKinesisSourceProvider;
import org.apache.flink.statefun.flink.io.spi.JsonIngressSpec;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.junit.Test;

public class RoutableProtobufKinesisSourceProviderTest {

  @Test
  public void exampleUsage() {
    JsonNode ingressDefinition =
        loadAsJsonFromClassResource(
            getClass().getClassLoader(), "routable-protobuf-kinesis-ingress.yaml");
    JsonIngressSpec<?> spec =
        new JsonIngressSpec<>(
            PolyglotKinesisIOTypes.ROUTABLE_PROTOBUF_KINESIS_INGRESS_TYPE,
            new IngressIdentifier<>(Message.class, "foo", "bar"),
            ingressDefinition);

    RoutableProtobufKinesisSourceProvider provider = new RoutableProtobufKinesisSourceProvider();
    SourceFunction<?> source = provider.forSpec(spec);

    assertThat(source, instanceOf(FlinkKinesisConsumer.class));
  }
}
