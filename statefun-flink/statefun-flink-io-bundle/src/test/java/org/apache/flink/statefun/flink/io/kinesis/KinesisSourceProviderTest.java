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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsCredentials;
import org.apache.flink.statefun.sdk.kinesis.ingress.IngressRecord;
import org.apache.flink.statefun.sdk.kinesis.ingress.KinesisIngressBuilder;
import org.apache.flink.statefun.sdk.kinesis.ingress.KinesisIngressDeserializer;
import org.apache.flink.statefun.sdk.kinesis.ingress.KinesisIngressSpec;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.junit.Test;

public class KinesisSourceProviderTest {

  private static final IngressIdentifier<String> ID =
      new IngressIdentifier<>(String.class, "namespace", "name");

  private static final String STREAM_NAME = "test-stream";

  @Test
  public void exampleUsage() {
    final KinesisIngressSpec<String> kinesisIngressSpec =
        KinesisIngressBuilder.forIdentifier(ID)
            .withAwsRegion("us-west-1")
            .withAwsCredentials(AwsCredentials.basic("access-key-id", "secret-access-key"))
            .withDeserializer(TestDeserializer.class)
            .withStream(STREAM_NAME)
            .build();

    final KinesisSourceProvider provider = new KinesisSourceProvider();
    final SourceFunction<String> source = provider.forSpec(kinesisIngressSpec);

    assertThat(source, instanceOf(FlinkKinesisConsumer.class));
  }

  private static final class TestDeserializer implements KinesisIngressDeserializer<String> {

    private static final long serialVersionUID = 1L;

    @Override
    public String deserialize(IngressRecord ingressRecord) {
      return null;
    }
  }
}
