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
package org.apache.flink.statefun.sdk.kinesis;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.kinesis.ingress.IngressRecord;
import org.apache.flink.statefun.sdk.kinesis.ingress.KinesisIngressBuilder;
import org.apache.flink.statefun.sdk.kinesis.ingress.KinesisIngressDeserializer;
import org.apache.flink.statefun.sdk.kinesis.ingress.KinesisIngressSpec;
import org.junit.Test;

public class KinesisIngressBuilderTest {

  private static final IngressIdentifier<String> ID =
      new IngressIdentifier<>(String.class, "namespace", "name");

  private static final String STREAM_NAME = "test-stream";

  @Test
  public void exampleUsage() {
    final KinesisIngressSpec<String> kinesisIngressSpec =
        KinesisIngressBuilder.forIdentifier(ID)
            .withDeserializer(TestDeserializer.class)
            .withStream(STREAM_NAME)
            .build();

    assertThat(kinesisIngressSpec.id(), is(ID));
    assertThat(kinesisIngressSpec.streams(), is(Collections.singletonList(STREAM_NAME)));
    assertTrue(kinesisIngressSpec.awsRegion().get().isDefault());
    assertTrue(kinesisIngressSpec.awsCredentials().get().isDefault());
    assertThat(kinesisIngressSpec.deserializer(), instanceOf(TestDeserializer.class));
    assertTrue(kinesisIngressSpec.startupPosition().isLatest());
    assertTrue(kinesisIngressSpec.properties().isEmpty());
  }

  private static final class TestDeserializer implements KinesisIngressDeserializer<String> {

    private static final long serialVersionUID = 1L;

    @Override
    public String deserialize(IngressRecord ingressRecord) {
      return null;
    }
  }
}
