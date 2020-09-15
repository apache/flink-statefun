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
import java.util.Objects;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

public final class MessageTypeSerializer extends TypeSerializer<Message> {

  private static final long serialVersionUID = 2L;

  // -- configuration --
  private final MessageFactoryKey messageFactoryKey;

  // -- runtime --
  private transient MessageFactory factory;

  MessageTypeSerializer(MessageFactoryKey messageFactoryKey) {
    this.messageFactoryKey = Objects.requireNonNull(messageFactoryKey);
  }

  @Override
  public boolean isImmutableType() {
    return false;
  }

  @Override
  public TypeSerializer<Message> duplicate() {
    return new MessageTypeSerializer(messageFactoryKey);
  }

  @Override
  public Message createInstance() {
    return null;
  }

  @Override
  public Message copy(Message message) {
    return message.copy(factory());
  }

  @Override
  public Message copy(Message message, Message reuse) {
    return message.copy(factory());
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public void serialize(Message message, DataOutputView dataOutputView) throws IOException {
    message.writeTo(factory(), dataOutputView);
  }

  @Override
  public Message deserialize(DataInputView dataInputView) throws IOException {
    return factory().from(dataInputView);
  }

  @Override
  public Message deserialize(Message message, DataInputView dataInputView) throws IOException {
    return deserialize(dataInputView);
  }

  @Override
  public void copy(DataInputView dataInputView, DataOutputView dataOutputView) throws IOException {
    factory().copy(dataInputView, dataOutputView);
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof MessageTypeSerializer;
  }

  @Override
  public int hashCode() {
    return getClass().hashCode();
  }

  @Override
  public TypeSerializerSnapshot<Message> snapshotConfiguration() {
    return new Snapshot(messageFactoryKey);
  }

  private MessageFactory factory() {
    if (factory == null) {
      factory = MessageFactory.forKey(messageFactoryKey);
    }
    return factory;
  }

  public static final class Snapshot implements TypeSerializerSnapshot<Message> {
    private MessageFactoryKey messageFactoryKey;

    @SuppressWarnings("unused")
    public Snapshot() {}

    Snapshot(MessageFactoryKey messageFactoryKey) {
      this.messageFactoryKey = messageFactoryKey;
    }

    @VisibleForTesting
    MessageFactoryKey getMessageFactoryKey() {
      return messageFactoryKey;
    }

    @Override
    public int getCurrentVersion() {
      return 2;
    }

    @Override
    public void writeSnapshot(DataOutputView dataOutputView) throws IOException {

      // version 1
      dataOutputView.writeUTF(messageFactoryKey.getType().name());

      // added in version 2
      writeNullableString(
          messageFactoryKey.getCustomPayloadSerializerClassName().orElse(null), dataOutputView);
    }

    @Override
    public void readSnapshot(int version, DataInputView dataInputView, ClassLoader classLoader)
        throws IOException {

      // read values and assign defaults appropriate for version 1
      MessageFactoryType messageFactoryType = MessageFactoryType.valueOf(dataInputView.readUTF());
      String customPayloadSerializerClassName = null;

      // if at least version 2, read in the custom payload serializer class name
      if (version >= 2) {
        customPayloadSerializerClassName = readNullableString(dataInputView);
      }

      this.messageFactoryKey =
          MessageFactoryKey.forType(messageFactoryType, customPayloadSerializerClassName);
    }

    @Override
    public TypeSerializer<Message> restoreSerializer() {
      return new MessageTypeSerializer(messageFactoryKey);
    }

    @Override
    public TypeSerializerSchemaCompatibility<Message> resolveSchemaCompatibility(
        TypeSerializer<Message> typeSerializer) {
      if (!(typeSerializer instanceof MessageTypeSerializer)) {
        return TypeSerializerSchemaCompatibility.incompatible();
      }
      MessageTypeSerializer casted = (MessageTypeSerializer) typeSerializer;
      if (casted.messageFactoryKey.equals(messageFactoryKey)) {
        return TypeSerializerSchemaCompatibility.compatibleAsIs();
      }
      return TypeSerializerSchemaCompatibility.incompatible();
    }

    private static void writeNullableString(String value, DataOutputView out) throws IOException {
      if (value != null) {
        out.writeBoolean(true);
        out.writeUTF(value);
      } else {
        out.writeBoolean(false);
      }
    }

    private static String readNullableString(DataInputView in) throws IOException {
      boolean isPresent = in.readBoolean();
      if (isPresent) {
        return in.readUTF();
      } else {
        return null;
      }
    }
  }
}
