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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Objects;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.statefun.flink.common.protobuf.ProtobufSerializer;
import org.apache.flink.statefun.flink.core.generated.Checkpoint;
import org.apache.flink.statefun.flink.core.generated.Envelope;
import org.apache.flink.statefun.flink.core.generated.Payload;
import org.apache.flink.statefun.sdk.Address;

public final class MessageFactory {

  public static MessageFactory forKey(MessageFactoryKey key) {
    return new MessageFactory(forPayloadKey(key));
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

  public Message from(Address from, Address to, Object payload, String messageId) {
    return new SdkMessage(from, to, payload, messageId);
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

  private static MessagePayloadSerializer forPayloadKey(MessageFactoryKey key) {
    switch (key.getType()) {
      case WITH_KRYO_PAYLOADS:
        return new MessagePayloadSerializerKryo();
      case WITH_PROTOBUF_PAYLOADS:
        return new MessagePayloadSerializerPb();
      case WITH_RAW_PAYLOADS:
        return new MessagePayloadSerializerRaw();
      case WITH_PROTOBUF_PAYLOADS_MULTILANG:
        return new MessagePayloadSerializerMultiLanguage();
      case WITH_CUSTOM_PAYLOADS:
        String className =
            key.getCustomPayloadSerializerClassName()
                .orElseThrow(
                    () ->
                        new UnsupportedOperationException(
                            "WITH_CUSTOM_PAYLOADS requires custom payload serializer class name to be specified in MessageFactoryKey"));
        return forCustomPayloadSerializer(className);
      default:
        throw new IllegalArgumentException("unknown serialization method " + key.getType());
    }
  }

  private static MessagePayloadSerializer forCustomPayloadSerializer(String className) {
    try {
      Class<?> clazz =
          Class.forName(className, true, Thread.currentThread().getContextClassLoader());
      Constructor<?> constructor = clazz.getConstructor();
      return (MessagePayloadSerializer) constructor.newInstance();
    } catch (Throwable ex) {
      throw new UnsupportedOperationException(
          String.format("Failed to create custom payload serializer: %s", className), ex);
    }
  }
}
