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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.kinesis.egress.EgressRecord;
import org.apache.flink.statefun.sdk.kinesis.egress.KinesisEgressBuilder;
import org.apache.flink.statefun.sdk.kinesis.egress.KinesisEgressSerializer;
import org.apache.flink.statefun.sdk.kinesis.egress.KinesisEgressSpec;
import org.junit.Test;

public class KinesisEgressBuilderTest {

  private static final EgressIdentifier<String> ID =
      new EgressIdentifier<>("namespace", "name", String.class);

  @Test
  public void exampleUsage() {
    final KinesisEgressSpec<String> kinesisEgressSpec =
        KinesisEgressBuilder.forIdentifier(ID).withSerializer(TestSerializer.class).build();

    assertThat(kinesisEgressSpec.id(), is(ID));
    assertTrue(kinesisEgressSpec.awsRegion().isDefault());
    assertTrue(kinesisEgressSpec.awsCredentials().isDefault());
    assertEquals(TestSerializer.class, kinesisEgressSpec.serializerClass());
    assertTrue(kinesisEgressSpec.clientConfigurationProperties().isEmpty());
  }

  private static final class TestSerializer implements KinesisEgressSerializer<String> {

    private static final long serialVersionUID = 1L;

    @Override
    public EgressRecord serialize(String value) {
      return null;
    }
  }
}
