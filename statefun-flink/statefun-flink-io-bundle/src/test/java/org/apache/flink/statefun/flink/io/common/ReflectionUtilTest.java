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
package org.apache.flink.statefun.flink.io.common;

import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.flink.statefun.sdk.kafka.KafkaIngressDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

public class ReflectionUtilTest {

  private static final class Serializer implements KafkaIngressDeserializer<String> {

    private static final long serialVersionUID = 1;

    @Override
    public String deserialize(ConsumerRecord<byte[], byte[]> input) {
      return null;
    }
  }

  @Test
  public void example() {
    Serializer serializer = ReflectionUtil.instantiate(Serializer.class);

    assertThat(serializer, CoreMatchers.notNullValue());
  }
}
