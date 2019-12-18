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

package com.ververica.statefun.flink.common.protobuf;

import com.google.protobuf.Message;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Objects;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

public final class ProtobufTypeSerializer<M extends Message> extends TypeSerializer<M> {

  private static final long serialVersionUID = 1;

  private final Class<M> typeClass;
  private transient ProtobufSerializer<M> underlyingSerializer;

  /** this is a lazy computed snapshot */
  @SuppressWarnings("InstanceVariableMayNotBeInitializedByReadObject")
  private transient ProtobufTypeSerializerSnapshot<M> snapshot;

  // --------------------------------------------------------------------------------------------------
  // Constructors
  // --------------------------------------------------------------------------------------------------

  ProtobufTypeSerializer(Class<M> typeClass) {
    this(typeClass, ProtobufSerializer.forMessageGeneratedClass(typeClass));
  }

  private ProtobufTypeSerializer(Class<M> typeClass, ProtobufSerializer<M> protobufSerializer) {
    this.typeClass = Objects.requireNonNull(typeClass);
    this.underlyingSerializer = Objects.requireNonNull(protobufSerializer);
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    this.underlyingSerializer = ProtobufSerializer.forMessageGeneratedClass(typeClass);
  }

  @Override
  public TypeSerializer<M> duplicate() {
    return new ProtobufTypeSerializer<>(typeClass, underlyingSerializer.duplicate());
  }

  @Override
  public boolean isImmutableType() {
    return true;
  }

  @Override
  public M createInstance() {
    return null;
  }

  @SuppressWarnings("unchecked")
  @Override
  public M copy(M from) {
    return (M) from.toBuilder().build();
  }

  @Override
  public M copy(M from, M reuse) {
    return copy(from);
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public void serialize(M record, DataOutputView target) throws IOException {
    underlyingSerializer.serialize(record, target);
  }

  @Override
  public M deserialize(DataInputView source) throws IOException {
    return underlyingSerializer.deserialize(source);
  }

  @Override
  public M deserialize(M reuse, DataInputView source) throws IOException {
    return deserialize(source);
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    underlyingSerializer.copy(source, target);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    Class<?> aClass = obj.getClass();
    return getClass().equals(aClass);
  }

  @Override
  public int hashCode() {
    return getClass().hashCode();
  }

  @Override
  public TypeSerializerSnapshot<M> snapshotConfiguration() {
    ProtobufTypeSerializerSnapshot<M> snapshot = this.snapshot;
    if (snapshot == null) {
      snapshot = new ProtobufTypeSerializerSnapshot<>(typeClass, underlyingSerializer.snapshot());
      this.snapshot = snapshot;
    }
    return snapshot;
  }

  Class<M> getTypeClass() {
    return typeClass;
  }
}
