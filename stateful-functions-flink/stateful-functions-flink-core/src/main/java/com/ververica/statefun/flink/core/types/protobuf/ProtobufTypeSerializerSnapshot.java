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

import com.google.protobuf.Message;
import com.ververica.statefun.flink.core.generated.ProtobufSerializerSnapshot;
import java.io.IOException;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

public final class ProtobufTypeSerializerSnapshot<M extends Message>
    implements TypeSerializerSnapshot<M> {

  private static final int VERSION = 1;

  @Nullable private ProtobufSerializerSnapshot snapshotData;

  @Nullable private Class<M> typeClass;

  @SuppressWarnings("unused")
  public ProtobufTypeSerializerSnapshot() {
    // used for reflective instantiation.
  }

  ProtobufTypeSerializerSnapshot(Class<M> messageType, ProtobufSerializerSnapshot snapshotData) {
    this.typeClass = Objects.requireNonNull(messageType);
    this.snapshotData = Objects.requireNonNull(snapshotData);
  }

  @SuppressWarnings("unchecked")
  private static <M extends Message> Class<M> classForName(
      ClassLoader userCodeClassLoader, ProtobufSerializerSnapshot snapshotData) {
    try {
      return (Class<M>)
          Class.forName(snapshotData.getGeneratedJavaName(), false, userCodeClassLoader);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(
          "Unable to restore the protobuf serializer since the generated java class is not found. "
              + "previously the generated java class was at "
              + snapshotData.getGeneratedJavaName()
              + " with "
              + snapshotData.getMessageName(),
          e);
    }
  }

  @Override
  public int getCurrentVersion() {
    return VERSION;
  }

  @Override
  public void writeSnapshot(DataOutputView out) throws IOException {
    if (!(snapshotData != null)) {
      throw new IllegalStateException();
    }
    out.writeInt(snapshotData.getSerializedSize());
    out.write(snapshotData.toByteArray());
  }

  @Override
  public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
      throws IOException {
    final int snapshotSize = in.readInt();
    final byte[] snapshotBytes = new byte[snapshotSize];
    in.readFully(snapshotBytes);

    this.snapshotData = ProtobufSerializerSnapshot.parseFrom(snapshotBytes);
    this.typeClass = classForName(userCodeClassLoader, snapshotData);
  }

  @Override
  public TypeSerializer<M> restoreSerializer() {
    Objects.requireNonNull(typeClass);
    return new ProtobufTypeSerializer<>(typeClass);
  }

  @Override
  public TypeSerializerSchemaCompatibility<M> resolveSchemaCompatibility(
      TypeSerializer<M> newSerializer) {
    if (!(newSerializer instanceof ProtobufTypeSerializer)) {
      return TypeSerializerSchemaCompatibility.incompatible();
    }
    ProtobufTypeSerializer<?> casted = (ProtobufTypeSerializer<?>) newSerializer;
    return resolveSchemaCompatibility(casted);
  }

  /**
   * Check schema compatibility with the new serializer.
   *
   * <p>This check is very simplistic, that just compares the two typeClasses, but the {@link
   * ProtobufSerializerSnapshot} has much more information to be used for compatibility resolution.
   * We make sure to store this information first, and implement a more robust schema resolution
   * logic in the future.
   */
  private TypeSerializerSchemaCompatibility<M> resolveSchemaCompatibility(
      ProtobufTypeSerializer<?> newSerializer) {
    Class<?> otherTypeClass = newSerializer.getTypeClass();
    if (otherTypeClass == typeClass) {
      return TypeSerializerSchemaCompatibility.compatibleAsIs();
    }
    return TypeSerializerSchemaCompatibility.incompatible();
  }
}
