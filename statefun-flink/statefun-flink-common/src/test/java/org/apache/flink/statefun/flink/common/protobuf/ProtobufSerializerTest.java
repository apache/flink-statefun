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
package org.apache.flink.statefun.flink.common.protobuf;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.protobuf.Message;
import java.io.IOException;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.statefun.flink.common.generated.ProtobufSerializerSnapshot;
import org.apache.flink.statefun.flink.common.protobuf.generated.TestProtos.SimpleMessage;
import org.junit.Test;

public class ProtobufSerializerTest {

  SimpleMessage originalMessage = SimpleMessage.newBuilder().setName("bob").build();

  @Test
  public void roundTrip() throws IOException {
    SimpleMessage message = roundTrip(SimpleMessage.class, originalMessage);

    assertThat(message, is(originalMessage));
  }

  @Test
  public void deserializeCopiedMessage() throws IOException {
    DataInputDeserializer in = serialize(SimpleMessage.class, originalMessage);

    ProtobufSerializer<SimpleMessage> serializer =
        ProtobufSerializer.forMessageGeneratedClass(SimpleMessage.class);
    DataOutputSerializer out = new DataOutputSerializer(32);

    serializer.copy(in, out);
    SimpleMessage message = deserialize(SimpleMessage.class, out);

    assertThat(message, is(originalMessage));
  }

  @Test
  public void testSnapshot() {
    ProtobufSerializer<SimpleMessage> serializer =
        ProtobufSerializer.forMessageGeneratedClass(SimpleMessage.class);
    ProtobufSerializerSnapshot snapshot = serializer.snapshot();

    assertThat(snapshot.getGeneratedJavaName(), is(SimpleMessage.class.getName()));
    assertThat(snapshot.getMessageName(), is(SimpleMessage.getDescriptor().getFullName()));
    assertThat(snapshot.getDescriptorSet(), notNullValue());
  }

  @Test
  public void duplicatedSerializerCanDeserialize() throws IOException {
    ProtobufSerializer<SimpleMessage> serializer =
        ProtobufSerializer.forMessageGeneratedClass(SimpleMessage.class);

    DataOutputSerializer out = new DataOutputSerializer(512);
    serializer.serialize(originalMessage, out);

    DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
    SimpleMessage foo = serializer.duplicate().deserialize(in);

    assertThat(foo, is(originalMessage));
  }

  @SuppressWarnings("SameParameterValue")
  private static <M extends Message> M roundTrip(Class<M> messageType, M original)
      throws IOException {
    DataInputDeserializer source = serialize(messageType, original);
    return deserialize(messageType, source);
  }

  @SafeVarargs
  private static <M extends Message> DataInputDeserializer serialize(Class<M> type, M... items)
      throws IOException {
    ProtobufSerializer<M> serializer = ProtobufSerializer.forMessageGeneratedClass(type);

    DataOutputSerializer out = new DataOutputSerializer(512);
    for (Object message : items) {
      serializer.serialize(type.cast(message), out);
    }
    return new DataInputDeserializer(out.getCopyOfBuffer());
  }

  @SuppressWarnings("SameParameterValue")
  private static <M extends Message> M deserialize(Class<M> type, DataOutputSerializer target)
      throws IOException {
    DataInputDeserializer source = new DataInputDeserializer(target.getCopyOfBuffer());
    return deserialize(type, source);
  }

  private static <M extends Message> M deserialize(Class<M> type, DataInputDeserializer source)
      throws IOException {
    ProtobufSerializer<M> serializer = ProtobufSerializer.forMessageGeneratedClass(type);
    return serializer.deserialize(source);
  }
}
