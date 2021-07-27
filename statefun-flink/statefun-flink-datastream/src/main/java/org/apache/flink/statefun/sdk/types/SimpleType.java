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
package org.apache.flink.statefun.sdk.types;

import java.util.Objects;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.slice.Slice;
import org.apache.flink.statefun.sdk.slice.Slices;

/**
 * A utility to create simple {@link Type} implementations.
 *
 * @param <T> the Java type handled by this {@link Type}.
 */
public final class SimpleType<T> implements Type<T> {

  @FunctionalInterface
  public interface Fn<I, O> {
    O apply(I input) throws Throwable;
  }

  public static <T> Type<T> simpleTypeFrom(
      TypeName typeName, Fn<T, byte[]> serialize, Fn<byte[], T> deserialize) {
    return new SimpleType<>(typeName, serialize, deserialize);
  }

  private final TypeName typeName;
  private final TypeSerializer<T> serializer;

  private SimpleType(TypeName typeName, Fn<T, byte[]> serialize, Fn<byte[], T> deserialize) {
    this.typeName = Objects.requireNonNull(typeName);
    this.serializer = new Serializer<>(serialize, deserialize);
  }

  @Override
  public TypeName typeName() {
    return typeName;
  }

  @Override
  public TypeSerializer<T> typeSerializer() {
    return serializer;
  }

  private static final class Serializer<T> implements TypeSerializer<T> {
    private final Fn<T, byte[]> serialize;
    private final Fn<byte[], T> deserialize;

    private Serializer(Fn<T, byte[]> serialize, Fn<byte[], T> deserialize) {
      this.serialize = Objects.requireNonNull(serialize);
      this.deserialize = Objects.requireNonNull(deserialize);
    }

    @Override
    public Slice serialize(T value) {
      try {
        byte[] bytes = serialize.apply(value);
        return Slices.wrap(bytes);
      } catch (Throwable throwable) {
        throw new IllegalStateException(throwable);
      }
    }

    @Override
    public T deserialize(Slice input) {
      try {
        byte[] bytes = input.toByteArray();
        return deserialize.apply(bytes);
      } catch (Throwable throwable) {
        throw new IllegalStateException(throwable);
      }
    }
  }
}
