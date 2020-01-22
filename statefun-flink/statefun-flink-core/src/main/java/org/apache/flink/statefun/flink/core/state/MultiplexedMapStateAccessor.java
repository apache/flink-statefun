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
package org.apache.flink.statefun.flink.core.state;

import java.io.IOException;
import java.util.Objects;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.statefun.sdk.state.Accessor;

final class MultiplexedMapStateAccessor<T> implements Accessor<T> {
  private final MapState<byte[], byte[]> mapStateHandle;
  private final byte[] accessorMapKey;
  private final RawSerializer<T> serializer;

  MultiplexedMapStateAccessor(
      MapState<byte[], byte[]> handle,
      byte[] accessorMapKey,
      TypeSerializer<T> subValueSerializer) {
    this.mapStateHandle = Objects.requireNonNull(handle);
    this.accessorMapKey = Objects.requireNonNull(accessorMapKey);
    this.serializer = new RawSerializer<>(subValueSerializer);
  }

  @Override
  public void set(T value) {
    try {
      if (value == null) {
        mapStateHandle.remove(accessorMapKey);
      } else {
        byte[] bytes = serializer.serialize(value);
        mapStateHandle.put(accessorMapKey, bytes);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public T get() {
    try {
      final byte[] bytes = mapStateHandle.get(accessorMapKey);
      if (bytes == null) {
        return null;
      }
      return serializer.deserialize(bytes);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void clear() {
    try {
      mapStateHandle.remove(accessorMapKey);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static final class RawSerializer<T> {
    private final TypeSerializer<T> delegate;
    private final DataOutputSerializer output;
    private final DataInputDeserializer input;

    RawSerializer(TypeSerializer<T> delegate) {
      this.delegate = Objects.requireNonNull(delegate);
      this.output = new DataOutputSerializer(32);
      this.input = new DataInputDeserializer();
    }

    byte[] serialize(T value) throws IOException {
      output.clear();
      delegate.serialize(value, output);
      return output.getCopyOfBuffer(); // TODO: consider avoiding buffer copying
    }

    T deserialize(byte[] bytes) throws IOException {
      input.setBuffer(bytes);
      final T value = delegate.deserialize(input);
      input.releaseArrays();
      return value;
    }
  }
}
