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

import java.io.IOException;
import java.util.Objects;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

public final class MessageTypeSerializer extends TypeSerializer<Message> {

  private static final long serialVersionUID = 1L;

  // -- configuration --
  private final MessageFactoryType messageFactoryType;

  // -- runtime --
  private transient MessageFactory factory;

  MessageTypeSerializer(MessageFactoryType messageFactoryType) {
    this.messageFactoryType = Objects.requireNonNull(messageFactoryType);
  }

  @Override
  public boolean isImmutableType() {
    return false;
  }

  @Override
  public TypeSerializer<Message> duplicate() {
    return new MessageTypeSerializer(messageFactoryType);
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
    return new Snapshot(messageFactoryType);
  }

  private MessageFactory factory() {
    if (factory == null) {
      factory = MessageFactory.forType(messageFactoryType);
    }
    return factory;
  }

  public static final class Snapshot implements TypeSerializerSnapshot<Message> {
    private MessageFactoryType messageFactoryType;

    @SuppressWarnings("unused")
    public Snapshot() {}

    Snapshot(MessageFactoryType messageFactoryType) {
      this.messageFactoryType = messageFactoryType;
    }

    @Override
    public int getCurrentVersion() {
      return 1;
    }

    @Override
    public void writeSnapshot(DataOutputView dataOutputView) throws IOException {
      dataOutputView.writeUTF(messageFactoryType.name());
    }

    @Override
    public void readSnapshot(int version, DataInputView dataInputView, ClassLoader classLoader)
        throws IOException {
      messageFactoryType = MessageFactoryType.valueOf(dataInputView.readUTF());
    }

    @Override
    public TypeSerializer<Message> restoreSerializer() {
      return new MessageTypeSerializer(messageFactoryType);
    }

    @Override
    public TypeSerializerSchemaCompatibility<Message> resolveSchemaCompatibility(
        TypeSerializer<Message> typeSerializer) {
      if (!(typeSerializer instanceof MessageTypeSerializer)) {
        return TypeSerializerSchemaCompatibility.incompatible();
      }
      MessageTypeSerializer casted = (MessageTypeSerializer) typeSerializer;
      if (casted.messageFactoryType == messageFactoryType) {
        return TypeSerializerSchemaCompatibility.compatibleAsIs();
      }
      return TypeSerializerSchemaCompatibility.incompatible();
    }
  }
}
