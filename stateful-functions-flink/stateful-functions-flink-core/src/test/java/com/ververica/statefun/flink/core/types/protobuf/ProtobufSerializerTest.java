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

package com.ververica.statefun.flink.core.types.protobuf;

import static com.ververica.statefun.flink.core.TestUtils.ADDRESS;
import static com.ververica.statefun.flink.core.TestUtils.envelopesOfVariousSizes;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.protobuf.Message;
import com.ververica.statefun.flink.core.generated.Envelope;
import com.ververica.statefun.flink.core.generated.EnvelopeAddress;
import com.ververica.statefun.flink.core.generated.ProtobufSerializerSnapshot;
import java.io.IOException;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.Test;

public class ProtobufSerializerTest {

  @Test
  public void roundTrip() throws IOException {
    EnvelopeAddress address = roundTrip(EnvelopeAddress.class, ADDRESS);

    assertThat(ADDRESS, is(address));
  }

  @Test
  public void deserializeCopiedMessage() throws IOException {
    DataInputDeserializer in = serialize(EnvelopeAddress.class, ADDRESS);

    ProtobufSerializer<EnvelopeAddress> serializer =
        ProtobufSerializer.forMessageGeneratedClass(EnvelopeAddress.class);
    DataOutputSerializer out = new DataOutputSerializer(32);

    serializer.copy(in, out);
    EnvelopeAddress message = deserialize(EnvelopeAddress.class, out);

    assertThat(message, is(ADDRESS));
  }

  @Test
  public void sequenceOfMessageIsDeserializeProperly() throws IOException {
    Envelope[] envelopes = envelopesOfVariousSizes();

    DataInputDeserializer in = serialize(Envelope.class, envelopes);

    for (Envelope originalEnvelope : envelopes) {
      Envelope envelope = deserialize(Envelope.class, in);
      assertThat(envelope, is(originalEnvelope));
    }
  }

  @Test
  public void testSnapshot() {
    ProtobufSerializer<EnvelopeAddress> serializer =
        ProtobufSerializer.forMessageGeneratedClass(EnvelopeAddress.class);
    ProtobufSerializerSnapshot snapshot = serializer.snapshot();

    assertThat(snapshot.getGeneratedJavaName(), is(EnvelopeAddress.class.getName()));
    assertThat(snapshot.getMessageName(), is(EnvelopeAddress.getDescriptor().getFullName()));
    assertThat(snapshot.getDescriptorSet(), notNullValue());
  }

  @Test
  public void duplicatedSerializerCanDeserialize() throws IOException {
    ProtobufSerializer<EnvelopeAddress> serializer =
        ProtobufSerializer.forMessageGeneratedClass(EnvelopeAddress.class);

    DataOutputSerializer out = new DataOutputSerializer(512);
    serializer.serialize(ADDRESS, out);

    DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
    EnvelopeAddress foo = serializer.duplicate().deserialize(in);

    assertThat(foo, is(ADDRESS));
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
