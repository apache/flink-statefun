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

package org.apache.flink.statefun.flink.core.types.remote;

import java.io.IOException;
import java.util.Objects;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.statefun.sdk.TypeName;

final class RemoteValueSerializer extends TypeSerializer<byte[]> {

  private static final long serialVersionUID = 1L;

  private static final byte[] EMPTY = new byte[0];

  private final TypeName type;

  public RemoteValueSerializer(TypeName type) {
    this.type = Objects.requireNonNull(type);
  }

  public TypeName getType() {
    return type;
  }

  @Override
  public boolean isImmutableType() {
    return false;
  }

  @Override
  public byte[] createInstance() {
    return EMPTY;
  }

  @Override
  public byte[] copy(byte[] from) {
    byte[] copy = new byte[from.length];
    System.arraycopy(from, 0, copy, 0, from.length);
    return copy;
  }

  @Override
  public byte[] copy(byte[] from, byte[] reuse) {
    return copy(from);
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public void serialize(byte[] record, DataOutputView target) throws IOException {
    if (record == null) {
      throw new IllegalArgumentException("The record must not be null.");
    }

    final int len = record.length;
    target.writeInt(len);
    target.write(record);
  }

  @Override
  public byte[] deserialize(DataInputView source) throws IOException {
    final int len = source.readInt();
    byte[] result = new byte[len];
    source.readFully(result);
    return result;
  }

  @Override
  public byte[] deserialize(byte[] reuse, DataInputView source) throws IOException {
    return deserialize(source);
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    final int len = source.readInt();
    target.writeInt(len);
    target.write(source, len);
  }

  @Override
  public TypeSerializer<byte[]> duplicate() {
    return new RemoteValueSerializer(type);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || RemoteValueSerializer.class != o.getClass()) return false;
    RemoteValueSerializer that = (RemoteValueSerializer) o;
    return Objects.equals(type, that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type);
  }

  @Override
  public TypeSerializerSnapshot<byte[]> snapshotConfiguration() {
    return new RemoteValueSerializerSnapshot(type);
  }
}
