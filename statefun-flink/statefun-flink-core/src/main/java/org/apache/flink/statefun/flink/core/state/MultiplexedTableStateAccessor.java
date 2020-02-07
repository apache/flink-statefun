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

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.statefun.flink.core.cache.SingleThreadedLruCache;
import org.apache.flink.statefun.flink.core.generated.MultiplexedStateKey;
import org.apache.flink.statefun.sdk.state.TableAccessor;

final class MultiplexedTableStateAccessor<K, V> implements TableAccessor<K, V> {
  private final MapState<MultiplexedStateKey, byte[]> mapStateHandle;
  private final MultiplexedStateKey accessorMapKeyPrefix;
  private final TaggedRawSerializer<K> keySerializer;
  private final TaggedRawSerializer<V> valueSerializer;

  private final SingleThreadedLruCache<K, MultiplexedStateKey> commonKeysCache =
      new SingleThreadedLruCache<>(128);

  MultiplexedTableStateAccessor(
      MapState<MultiplexedStateKey, byte[]> handle,
      MultiplexedStateKey accessorMapKeyPrefix,
      TypeSerializer<K> subKeySerializer,
      TypeSerializer<V> subValueSerializer) {
    this.mapStateHandle = Objects.requireNonNull(handle);
    this.keySerializer = new TaggedRawSerializer<>(new byte[0], subKeySerializer);
    this.valueSerializer = new TaggedRawSerializer<>(new byte[0], subValueSerializer);
    this.accessorMapKeyPrefix = accessorMapKeyPrefix;
  }

  @Override
  public void set(K key, V value) {
    try {
      MultiplexedStateKey keyBytes = stateKey(key);
      if (value == null) {
        mapStateHandle.remove(keyBytes);
      } else {
        byte[] bytes = valueSerializer.serialize(value);
        mapStateHandle.put(keyBytes, bytes);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public V get(K userKey) {
    try {
      final MultiplexedStateKey stateKey = stateKey(userKey);
      final byte[] bytes = mapStateHandle.get(stateKey);
      if (bytes == null) {
        return null;
      }
      return valueSerializer.deserialize(bytes);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void remove(K userKey) {
    try {
      mapStateHandle.remove(stateKey(userKey));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  private MultiplexedStateKey stateKey(final K userKey) {
    Objects.requireNonNull(userKey, "Key can not be NULL");
    @Nullable MultiplexedStateKey stateKey = commonKeysCache.get(userKey);
    if (stateKey != null) {
      return stateKey;
    }
    try {
      commonKeysCache.put(userKey, stateKey = computeStateKeyFromUserKey(userKey));
      return stateKey;
    } catch (IOException e) {
      throw new RuntimeException("Unable to serialize the key " + userKey, e);
    }
  }

  private MultiplexedStateKey computeStateKeyFromUserKey(K userKey) throws IOException {
    byte[] userKeyBytes = keySerializer.serialize(userKey);
    ByteString userKeyByteString = ByteString.copyFrom(userKeyBytes);
    return accessorMapKeyPrefix.toBuilder().addUserKeys(userKeyByteString).build();
  }

  private static final class TaggedRawSerializer<T> {

    private final TypeSerializer<T> delegate;
    private final DataOutputSerializer output;
    private final DataInputDeserializer input;
    private final byte[] tag;

    TaggedRawSerializer(byte[] tag, TypeSerializer<T> delegate) {
      this.tag = Objects.requireNonNull(tag);
      this.delegate = Objects.requireNonNull(delegate);
      this.output = new DataOutputSerializer(32 + tag.length);
      this.input = new DataInputDeserializer();
    }

    byte[] serialize(T value) throws IOException {
      output.clear();
      output.write(tag);
      delegate.serialize(value, output);
      return output.getCopyOfBuffer(); // TODO: consider avoiding buffer copying
    }

    T deserialize(byte[] bytes) throws IOException {
      input.setBuffer(bytes);
      input.skipBytesToRead(tag.length);
      final T value = delegate.deserialize(input);
      input.releaseArrays();
      return value;
    }
  }
}
