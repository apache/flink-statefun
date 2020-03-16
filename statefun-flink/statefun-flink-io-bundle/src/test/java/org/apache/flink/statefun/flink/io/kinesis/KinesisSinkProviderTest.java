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
import static org.junit.Assert.assertThat;

import java.nio.ByteBuffer;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.kinesis.auth.AwsCredentials;
import org.apache.flink.statefun.sdk.kinesis.egress.KinesisEgressBuilder;
import org.apache.flink.statefun.sdk.kinesis.egress.KinesisEgressSerializer;
import org.apache.flink.statefun.sdk.kinesis.egress.KinesisEgressSpec;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.junit.Test;

public class KinesisSinkProviderTest {

  private static final EgressIdentifier<String> ID =
      new EgressIdentifier<>("namespace", "name", String.class);

  @Test
  public void exampleUsage() {
    final KinesisEgressSpec<String> kinesisEgressSpec =
        KinesisEgressBuilder.forIdentifier(ID)
            .withAwsRegion("us-west-1")
            .withAwsCredentials(AwsCredentials.basic("access-key-id", "secret-access-key"))
            .withSerializer(TestSerializer.class)
            .build();

    final KinesisSinkProvider provider = new KinesisSinkProvider();
    final SinkFunction<String> sink = provider.forSpec(kinesisEgressSpec);

    assertThat(sink, instanceOf(FlinkKinesisProducer.class));
  }

  private static final class TestSerializer implements KinesisEgressSerializer<String> {

    private static final long serialVersionUID = 1L;

    @Override
    public ByteBuffer serialize(String record) {
      return null;
    }

    @Override
    public String partitionKey(String record) {
      return null;
    }

    @Override
    public String targetStream(String record) {
      return null;
    }
  }
}
