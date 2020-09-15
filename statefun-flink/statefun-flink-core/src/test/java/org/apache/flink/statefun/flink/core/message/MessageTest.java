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
package org.apache.flink.statefun.flink.core.message;

import static org.apache.flink.statefun.flink.core.TestUtils.*;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import com.google.protobuf.Any;
import java.io.IOException;
import java.util.Arrays;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class MessageTest {
  private final MessageFactoryType type;
  private final String customPayloadSerializerClassName;
  private final Object payload;

  public MessageTest(
      MessageFactoryType type, String customPayloadSerializerClassName, Object payload) {
    this.type = type;
    this.customPayloadSerializerClassName = customPayloadSerializerClassName;
    this.payload = payload;
  }

  @Parameters(name = "{0}")
  public static Iterable<? extends Object[]> data() {
    return Arrays.asList(
        new Object[] {MessageFactoryType.WITH_KRYO_PAYLOADS, null, DUMMY_PAYLOAD},
        new Object[] {MessageFactoryType.WITH_PROTOBUF_PAYLOADS, null, DUMMY_PAYLOAD},
        new Object[] {MessageFactoryType.WITH_RAW_PAYLOADS, null, DUMMY_PAYLOAD.toByteArray()},
        new Object[] {
          MessageFactoryType.WITH_PROTOBUF_PAYLOADS_MULTILANG, null, Any.pack(DUMMY_PAYLOAD)
        },
        new Object[] {
          MessageFactoryType.WITH_CUSTOM_PAYLOADS,
          "org.apache.flink.statefun.flink.core.message.JavaPayloadSerializer",
          DUMMY_PAYLOAD
        });
  }

  @Test
  public void roundTrip() throws IOException {
    MessageFactory factory =
        MessageFactory.forKey(MessageFactoryKey.forType(type, customPayloadSerializerClassName));

    Message fromSdk = factory.from(FUNCTION_1_ADDR, FUNCTION_2_ADDR, payload);
    DataOutputSerializer out = new DataOutputSerializer(32);
    fromSdk.writeTo(factory, out);

    Message fromEnvelope = factory.from(new DataInputDeserializer(out.getCopyOfBuffer()));

    assertThat(fromEnvelope.source(), is(FUNCTION_1_ADDR));
    assertThat(fromEnvelope.target(), is(FUNCTION_2_ADDR));

    ClassLoader targetClassLoader = payload.getClass().getClassLoader();
    Object payload = fromEnvelope.payload(factory, targetClassLoader);

    assertThat(payload, is(this.payload));
  }
}
