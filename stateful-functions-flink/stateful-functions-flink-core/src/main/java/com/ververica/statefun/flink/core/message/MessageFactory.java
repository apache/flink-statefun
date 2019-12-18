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

package com.ververica.statefun.flink.core.message;

import com.ververica.statefun.flink.common.protobuf.ProtobufSerializer;
import com.ververica.statefun.flink.core.generated.Checkpoint;
import com.ververica.statefun.flink.core.generated.Envelope;
import com.ververica.statefun.flink.core.generated.Payload;
import com.ververica.statefun.sdk.Address;
import java.io.IOException;
import java.util.Objects;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

public final class MessageFactory {

  public static MessageFactory forType(MessageFactoryType type) {
    return new MessageFactory(forPayloadType(type));
  }

  private final ProtobufSerializer<Envelope> envelopeSerializer;
  private final MessagePayloadSerializer userMessagePayloadSerializer;

  private MessageFactory(MessagePayloadSerializer userMessagePayloadSerializer) {
    this.envelopeSerializer = ProtobufSerializer.forMessageGeneratedClass(Envelope.class);
    this.userMessagePayloadSerializer = Objects.requireNonNull(userMessagePayloadSerializer);
  }

  public Message from(long checkpointId) {
    return from(envelopeWithCheckpointId(checkpointId));
  }

  public Message from(DataInputView input) throws IOException {
    return from(deserializeEnvelope(input));
  }

  public Message from(Address from, Address to, Object payload) {
    return new SdkMessage(from, to, payload);
  }

  // -------------------------------------------------------------------------------------------------------

  void copy(DataInputView source, DataOutputView target) throws IOException {
    copyEnvelope(source, target);
  }

  private Message from(Envelope envelope) {
    return new ProtobufMessage(envelope);
  }

  Payload serializeUserMessagePayload(Object payloadObject) {
    return userMessagePayloadSerializer.serialize(payloadObject);
  }

  Object deserializeUserMessagePayload(ClassLoader targetClassLoader, Payload payload) {
    return userMessagePayloadSerializer.deserialize(targetClassLoader, payload);
  }

  Object copyUserMessagePayload(ClassLoader targetClassLoader, Object payload) {
    return userMessagePayloadSerializer.copy(targetClassLoader, payload);
  }

  void serializeEnvelope(Envelope envelope, DataOutputView target) throws IOException {
    envelopeSerializer.serialize(envelope, target);
  }

  private Envelope deserializeEnvelope(DataInputView source) throws IOException {
    return envelopeSerializer.deserialize(source);
  }

  private void copyEnvelope(DataInputView source, DataOutputView target) throws IOException {
    envelopeSerializer.copy(source, target);
  }

  private static Envelope envelopeWithCheckpointId(long checkpointId) {
    Checkpoint checkpoint = Checkpoint.newBuilder().setCheckpointId(checkpointId).build();

    return Envelope.newBuilder().setCheckpoint(checkpoint).build();
  }

  private static MessagePayloadSerializer forPayloadType(MessageFactoryType type) {
    switch (type) {
      case WITH_KRYO_PAYLOADS:
        return new MessagePayloadSerializerKryo();
      case WITH_PROTOBUF_PAYLOADS:
        return new MessagePayloadSerializerPb();
      case WITH_RAW_PAYLOADS:
        return new MessagePayloadSerializerRaw();
      default:
        throw new IllegalArgumentException("unknown serialization method " + type);
    }
  }
}
